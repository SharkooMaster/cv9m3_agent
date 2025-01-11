using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Agent.Interfaces;
using Agent.Models;
using Agent.Modules.Agneta;
using Agent.Utils;
using Agent.Utils.Globals;
using Grpc.Core;

namespace Agent.Modules.Peer
{
    public static class NodeService
    {
        // GRPC services
        public static FindPeerResponsibleService _findPeerResponsible = new FindPeerResponsibleService();
        public static GetNodeInfoService _getNodeInfoService         = new GetNodeInfoService();
        public static GetPredecessorService _getPredecessorService   = new GetPredecessorService();
        public static GetSuccessorService _getSuccessorService       = new GetSuccessorService();
        public static GetHealthService _getHealth                   = new GetHealthService();

        public static UpdateSuccessorService _updateSuccessorService   = new UpdateSuccessorService();
        public static UpdatePredecessorService _updatePredecessorService = new UpdatePredecessorService();
        public static UpdateFingerTableService _updateFingerTableService = new UpdateFingerTableService();

        // Keeps track of which finger index we'll fix next
        public static int _nextFinger = 0;

        /// <summary>
        /// Join this node to the network. If bootstrap_node is null, it becomes the only node.
        /// Otherwise it looks up the node responsible for "node.id" starting from bootstrap_node,
        /// sets its own successor/predecessor accordingly, and notifies the relevant nodes.
        /// </summary>
        public static async Task<M_Node> JoinNetwork(M_Node node, string bootstrap_node)
        {
            try
            {
                if (bootstrap_node == null)
                {
                    // If no bootstrap, this is the only node in the ring
                    await AgnetaHandler.Log(0, "Only node in the network");
                    node.successor   = new M_Node { id = node.id, ip = node.ip };
                    node.predecessor = new M_Node { id = node.id, ip = node.ip };
                    node = await InitFingerTable(node);
                    return node;
                }

                // 1) Find my immediate successor by asking the bootstrap node
                string successor_ip = null;
                for (int attempt = 0; attempt < 3 && successor_ip == null; attempt++)
                {
                    try
                    {
                        successor_ip = await S_FindPeerResponsible(node.id, bootstrap_node);
                        if (string.IsNullOrEmpty(successor_ip))
                            throw new Exception("Empty successor IP");
                    }
                    catch (Exception ex)
                    {
                        await AgnetaHandler.Log(1, $"Join attempt {attempt + 1} failed: {ex.Message}");
                        await Task.Delay(100 * (attempt + 1));
                    }
                }

                if (successor_ip == null)
                {
                    throw new Exception("Failed to join network - could not find successor");
                }

                // 2) Set my successor
                var getSuccessorRes = await _getNodeInfoService.ClientGet(successor_ip);
                node.successor = new M_Node { id = getSuccessorRes.Id, ip = getSuccessorRes.Ip };

                // 3) Get the successor's predecessor -> that becomes my predecessor
                GetPredecessor_Result getPredRes;
                try
                {
                    getPredRes = await _getPredecessorService.ClientGet(node.successor.ip);
                }
                catch
                {
                    throw new Exception("Failed to get predecessor from successor");
                }

                node.predecessor = new M_Node { id = getPredRes.Id, ip = getPredRes.Ip };

                // 4) Let my successor know I'm its new predecessor
                var updatePredReq = new UpdatePredecessor_Req { Id = node.id, Ip = node.ip };
                await _updatePredecessorService.ClientUpdate(updatePredReq, node.successor.ip);

                // 5) Let my predecessor know I'm its new successor
                var updateSuccReq = new UpdateSuccessor_Req { Id = node.id, Ip = node.ip };
                await _updateSuccessorService.ClientUpdate(updateSuccReq, node.predecessor.ip);

                // 6) Build my finger table
                node = await InitFingerTable(node);
                node = await BuildFingerTable(node);

                // 7) Notify fingers
                await NotifyFingersOfNewNode(node);

                return node;
            }
            catch (Exception ex)
            {
                await AgnetaHandler.Log(1, $"JoinNetwork failed: {ex.Message}");
                throw;  // Let caller handle
            }
        }

        /// <summary>
        /// Notify relevant fingers that a new node has joined. This is typical "updateFingerTable" logic.
        /// </summary>
        public static async Task NotifyFingersOfNewNode(M_Node node)
        {
            // For each finger index, we figure out which node might need to update that finger
            for (int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
            {
                try
                {
                    // Calculate the ID that would point to 'node' from 'some predecessor' at finger i
                    ulong backwardDistance = (1UL << i);
                    ulong potentialPredecessorId = (node.id >= backwardDistance)
                        ? (node.id - backwardDistance)
                        : ((1UL << Globals.FINGER_TABLE_SIZE) - (backwardDistance - node.id));

                    // Find the node that claims responsibility for potentialPredecessorId
                    string predecessorIp = await FindSuccessor(node, potentialPredecessorId);

                    // If it's not me or my immediate predecessor, we ask it to update its finger
                    if (predecessorIp != node.ip &&
                        (node.predecessor == null || predecessorIp != node.predecessor.ip))
                    {
                        await _updateFingerTableService.ClientUpdate(
                            new UpdateFingerTable_Req
                            {
                                FingerIndex = i,
                                Id = node.id,
                                Ip = node.ip
                            },
                            predecessorIp
                        );
                    }
                }
                catch (Exception ex)
                {
                    await AgnetaHandler.Log(1, $"NotifyFingersOfNewNode: finger {i} failed: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Initialize your finger table so that it has "something" for each key.
        /// Typically for the single-node ring or brand new node that hasn't built a full table yet.
        /// </summary>
        private static async Task<M_Node> InitFingerTable(M_Node node)
        {
            // Just fill every finger with our own node as a default
            for (int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
            {
                ulong fingerId = (node.id + (1UL << i)) % (1UL << Globals.FINGER_TABLE_SIZE);
                node.fingerTable.AddOrUpdate(
                    fingerId,
                    new M_Node { id = node.id, ip = node.ip },
                    (k, oldVal) => new M_Node { id = node.id, ip = node.ip }
                );
            }
            return node;
        }

        /// <summary>
        /// Build your actual finger table by calling FindSuccessor on each fingerId.
        /// This should be done after you have a known successor and predecessor.
        /// </summary>
        private static async Task<M_Node> BuildFingerTable(M_Node node)
        {
            try
            {
                for (int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
                {
                    ulong fingerId = (node.id + (1UL << i)) % (1UL << Globals.FINGER_TABLE_SIZE);

                    string responsibleIp = null;
                    try
                    {
                        responsibleIp = await FindSuccessor(node, fingerId);
                    }
                    catch (Exception ex)
                    {
                        await AgnetaHandler.Log(1, $"BuildFingerTable: finger {i} findSuccessor failed: {ex.Message}");
                        continue;
                    }

                    if (string.IsNullOrEmpty(responsibleIp)) continue;

                    try
                    {
                        var nodeInfo = await _getNodeInfoService.ClientGet(responsibleIp);
                        var newFinger = new M_Node { id = nodeInfo.Id, ip = nodeInfo.Ip };
                        // Insert or update
                        node.fingerTable.AddOrUpdate(
                            fingerId,
                            newFinger,
                            (k, oldVal) => newFinger
                        );
                    }
                    catch (Exception ex)
                    {
                        await AgnetaHandler.Log(1, $"BuildFingerTable: finger {i} retrieval failed: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                await AgnetaHandler.Log(1, $"BuildFingerTable: Critical error: {ex.Message}");
            }

            return node;
        }

        /// <summary>
        /// gRPC call to find the peer responsible for 'target' (remote method).
        /// </summary>
        private static async Task<string> S_FindPeerResponsible(ulong target, string ip)
        {
            var req = new QueryReq { Val = target };
            var res = await _findPeerResponsible.ClientFind(req, ip);
            return res.Res;
        }

        /// <summary>
        /// Local method to find the successor of a key 'target' using this node's knowledge.
        /// If the key is between this.predecessor and this node, it's me.
        /// If it's between this node and my successor, it's my successor.
        /// Else, forward the request to closest preceding node.
        /// </summary>
        public static async Task<string> FindSuccessor(M_Node node, ulong target)
        {
            if (node.predecessor != null && NodeUtils.inBetween(target, node.predecessor.id, node.id))
            {
                // I'm responsible
                return node.ip;
            }
            else if (node.successor != null && NodeUtils.inBetween(target, node.id, node.successor.id))
            {
                // My successor is responsible
                return node.successor.ip;
            }
            else
            {
                // Ask the closest preceding node to forward
                M_Node peer = await ClosestPreceedingNode(node, target);
                return await S_FindPeerResponsible(target, peer.ip);
            }
        }

        /// <summary>
        /// Return the finger in [fingerTable] that most closely precedes 'target'.
        /// For correctness with a dictionary, we must sort keys in descending order and check them.
        /// </summary>
        private static async Task<M_Node> ClosestPreceedingNode(M_Node node, ulong target)
        {
            // Sort the finger IDs in descending order to replicate "for i = m-1 downto 0"
            var fingerKeysDesc = node.fingerTable.Keys.OrderByDescending(k => k).ToArray();

            foreach (var fingerKey in fingerKeysDesc)
            {
                M_Node candidate = node.fingerTable[fingerKey];
                if (candidate != null && NodeUtils.inBetween(candidate.id, node.id, target))
                {
                    return candidate;
                }
            }
            // Fallback: if none is preceding, return myself
            return node;
        }

        /// <summary>
        /// Verify our successor hasn't changed behind our back. Typical "Stabilize" logic.
        /// If the successor's predecessor is neither null nor us, adopt them as the new successor.
        /// Then tell them we are their predecessor.
        /// </summary>
        public static async Task<M_Node> VerifySuccessor(M_Node node)
        {
            try
            {
                if (node?.successor == null) return node;

                GetPredecessor_Result getPredecessorRes;
                try
                {
                    getPredecessorRes = await _getPredecessorService.ClientGet(node.successor.ip);
                    if (getPredecessorRes == null) return node;
                }
                catch (Exception ex)
                {
                    await AgnetaHandler.Log(1, $"VerifySuccessor: Can't get predecessor: {ex.Message}");
                    return node;
                }

                // If that predecessor is not me, adopt it
                if (getPredecessorRes.Id != node.id || getPredecessorRes.Ip != node.ip)
                {
                    // Potential new successor
                    var newSuccessor = new M_Node { id = getPredecessorRes.Id, ip = getPredecessorRes.Ip };
                    if (newSuccessor.id != 0)
                    {
                        node.successor = newSuccessor;
                        try
                        {
                            await _updatePredecessorService.ClientUpdate(
                                new UpdatePredecessor_Req { Id = node.id, Ip = node.ip },
                                node.successor.ip
                            );
                        }
                        catch (Exception ex)
                        {
                            await AgnetaHandler.Log(1, $"VerifySuccessor: Updating predecessor failed: {ex.Message}");
                        }
                    }
                }

                return node;
            }
            catch (Exception ex)
            {
                await AgnetaHandler.Log(1, $"VerifySuccessor: Critical error: {ex.Message}");
                return node;
            }
        }

        /// <summary>
        /// Fix one entry of the finger table each time. Typical "Fix Fingers" logic in Chord.
        /// Moves through the finger indices in a round-robin manner.
        /// </summary>
        public static async Task<M_Node> FixFingerTable(M_Node node)
        {
            // If we haven't built the entire dictionary yet, skip
            if (node.fingerTable.Count != Globals.FINGER_TABLE_SIZE)
                return node;

            _nextFinger = (_nextFinger + 1) % Globals.FINGER_TABLE_SIZE;

            // The key in the ring for this finger
            ulong fingerKey = (node.id + (1UL << _nextFinger)) % (1UL << Globals.FINGER_TABLE_SIZE);

            // Find the node responsible for 'fingerKey'
            string successorIp = await FindSuccessor(node, fingerKey);
            if (successorIp == node.ip)
            {
                // It's me, do nothing
                return node;
            }

            // If it's my known successor, short-circuit
            if (node.successor != null && successorIp == node.successor.ip)
            {
                var sameEntry = new M_Node { id = node.successor.id, ip = node.successor.ip };
                node.fingerTable.AddOrUpdate(fingerKey, sameEntry, (k, oldVal) => sameEntry);
                return node;
            }

            // Otherwise, fetch info from that successor
            var successorInfo = await _getNodeInfoService.ClientGet(successorIp);
            var newFingerEntry = new M_Node
            {
                id = successorInfo.Id,
                ip = successorInfo.Ip
            };

            // Update the dictionary
            node.fingerTable.AddOrUpdate(fingerKey, newFingerEntry, (k, oldVal) => newFingerEntry);

            return node;
        }
    }
}