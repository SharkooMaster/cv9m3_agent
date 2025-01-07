using Agent.Interfaces;
using Agent.Models;
using Agent.Modules.Agneta;
using Agent.Utils;
using Agent.Utils.Globals;

namespace Agent.Modules.Peer;

public static class NodeService
{
    public static FindPeerResponsibleService _findPeerResponsible = new FindPeerResponsibleService();

    public static GetNodeInfoService _getNodeInfoService = new GetNodeInfoService();
    public static GetPredecessorService _getPredecessorService = new GetPredecessorService();
    public static GetSuccessorService _getSuccessorService = new GetSuccessorService();
    public static GetHealthService _getHealth = new GetHealthService();

    public static UpdateSuccessorService _updateSuccessorService = new UpdateSuccessorService();
    public static UpdatePredecessorService _updatePredecessorService = new UpdatePredecessorService();
    public static UpdateFingerTableService _updateFingerTableService = new UpdateFingerTableService();

    public static async Task UpdateOthers(M_Node _node)
    {
        ulong m = 1UL << Globals.FINGER_TABLE_SIZE;
    
        for (int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
        {
            // Find last node whose ith finger might be n
            ulong p = (_node.id - (1UL << i) + m) % m;  // Adding m before mod to handle underflow

            string predIp = await FindPredecessor(_node, p);
            if (predIp != _node.ip)
            {
                await _updateFingerTableService.ClientUpdate(
                    new UpdateFingerTable_Req() {
                        FingerIndex = i,
                        Id = _node.id,
                        Ip = _node.ip
                    },
                    predIp
                );
            }
        }
    }

    public static async Task<M_Node> UpdateFingerTable(M_Node _node, M_Node new_node, int _fingerIndex)
    {
        ulong[] fingerTableKeys = _node.fingerTable.Keys.ToArray();
        _node.fingerTable[fingerTableKeys[_fingerIndex]] = new_node;
        return _node;
    }

    public static async Task<string> FindSuccessor(M_Node _node, ulong id)
    {
        // Handle wrap-around
        if (_node.successor.ip == _node.ip)
            return _node.ip;

        // Check if id is between us and successor
        if (NodeUtils.inBetween(id, _node.id, _node.successor.id) || id == _node.id)
            return _node.successor.ip;

        // Find closest preceding node
        var n = ClosestPrecedingFinger(_node, id);
        if (n.ip == _node.ip)
            return _node.successor.ip;

        // Forward the query
        QueryReq req = new QueryReq() { Val = id };
        QueryRes res = await _findPeerResponsible.ClientFind(req, n.ip);
        return res.Res;
    }

    public static async Task<M_Node> Stabilize(M_Node _node)
    {
        // Periodically verify successor
        // and notify it about this node
        try
        {
            // Get successor's predecessor
            GetPredecessor_Result x = await _getPredecessorService.ClientGet(_node.successor.ip);

            // Check if it should be our new successor
            if (x != null && NodeUtils.inBetween(x.Id, _node.id, _node.successor.id))
            {
                _node.successor = new M_Node() { id = x.Id, ip = x.Ip };

                // Update successor list
                _node.successor_list.Insert(0, _node.successor);
                if (_node.successor_list.Count > Globals.SUCCESSOR_LIST_SIZE)
                    _node.successor_list.RemoveAt(_node.successor_list.Count - 1);
            }

            // Notify successor about us
            await _updatePredecessorService.ClientUpdate(
                new UpdatePredecessor_Req() { Id = _node.id, Ip = _node.ip },
                _node.successor.ip
            );

            // Update successor list by getting successor's list
            _node = await UpdateSuccessorList(_node);
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(1, $"Stabilize failed: {ex.Message}");
            // Handle successor failure by using successor list
            if (_node.successor_list.Count > 1)
            {
                _node.successor = _node.successor_list[1];
                _node.successor_list.RemoveAt(0);
            }
        }

        return _node;

    }

    public static int nextFingerToStabalize = 0;
    public static async Task<M_Node> FixFingers(M_Node _node)
    {
        // Periodically refresh finger table entries
        try
        {
            nextFingerToStabalize = (nextFingerToStabalize + 1) % Globals.FINGER_TABLE_SIZE;

            ulong start = (_node.id + (1UL << nextFingerToStabalize)) % (1UL << Globals.FINGER_TABLE_SIZE);
            string newSuccessorIp = await FindSuccessor(_node, start);

            if (newSuccessorIp != _node.ip)
            {
                GetNodeInfo_Result nodeInfo = await _getNodeInfoService.ClientGet(newSuccessorIp);
                _node.fingerTable[start] = new M_Node() { 
                    id = nodeInfo.Id, 
                    ip = nodeInfo.Ip 
                };
                await AgnetaHandler.Log(1, $"Updated finger {nextFingerToStabalize} to node {nodeInfo.Id}");
            }
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(1, $"FixFingers failed: {ex.Message}");
        }

        return _node;

    }

    public static async Task<M_Node> CheckPredecessor(M_Node _node)
    {
        // Periodically check if predecessor is alive 
        if(_node.predecessor != null && _node.predecessor.ip != _node.ip)
        {
            try
            {
                GetHealth_Result res = await _getHealth.ClientGet(_node.predecessor.ip);
                if(res.Status != "Healthy")
                {
                    await AgnetaHandler.Log(1, $"CheckPredecessor failed, status: {res.Status}");
                }
            }
            catch(TimeoutException)
            {
                await AgnetaHandler.Log(1, $"Predecessor {_node.predecessor.id} : {_node.predecessor.ip} timedout");
                _node.predecessor = null;
            }
            catch(Exception ex)
            {
                await AgnetaHandler.Log(1, $"Failed to check predecessor: {ex.Message}");
                _node.predecessor = null;
            }
        }
        return _node;
    }

    public static async Task<string> FindPredecessor(M_Node _node, ulong id)
    {
        if (_node == null)
        {
            throw new ArgumentNullException(nameof(_node), "Node cannot be null");
        }

        // If we're the only node in the network
        if (_node.successor == null || _node.successor.ip == _node.ip)
        {
            return _node.ip;
        }

        // Initialize current node
        M_Node current = new M_Node { id = _node.id, ip = _node.ip };
        M_Node successor = _node.successor;

        int maxAttempts = Globals.FINGER_TABLE_SIZE; // Prevent infinite loops
        int attempts = 0;

        try
        {
            while (!NodeUtils.inBetween(id, current.id, successor.id) && 
                   id != successor.id && 
                   attempts < maxAttempts)
            {
                attempts++;

                // Get closest preceding finger
                var closestNode = ClosestPrecedingFinger(current, id);
                if (closestNode == null || closestNode.ip == current.ip)
                {
                    break;
                }

                // Update current node
                current = closestNode;

                // Get successor of closest node
                try
                {
                    var succRes = await RetryRPC(async () => 
                        await _getSuccessorService.ClientGet(current.ip));

                    if (succRes == null)
                    {
                        await AgnetaHandler.Log(1, "GetSuccessor returned null");
                        break;
                    }

                    successor = new M_Node { id = succRes.id, ip = succRes.ip };
                }
                catch (Exception ex)
                {
                    await AgnetaHandler.Log(1, $"Failed to get successor for {current.ip}: {ex.Message}");
                    break;
                }
            }

            return current.ip;
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(1, $"FindPredecessor encountered an error: {ex.Message}");
            throw;
        }
    }

    // Add periodic maintenance scheduler
    public static async Task StartMaintenanceTasks(M_Node _node, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Run stabilize every 1 second
                await Task.Delay(1000, cancellationToken);
                _node = await Stabilize(_node);

                // Run finger fixing every 2 seconds
                if (DateTime.Now.Second % 2 == 0)
                {
                    _node = await FixFingers(_node);
                }

                // Check predecessor every 3 seconds
                if (DateTime.Now.Second % 3 == 0)
                {
                    _node = await CheckPredecessor(_node);
                }

                Globals._NODE = _node;
            }
            catch (Exception ex)
            {
                await AgnetaHandler.Log(1, $"Maintenance task failed: {ex.Message}");
                // Continue running maintenance even if individual tasks fail
            }
        }
    }

    // Improve UpdateSuccessorList to handle full successor list
    private static async Task<M_Node> UpdateSuccessorList(M_Node _node)
    {
        try
        {
            var newList = new List<M_Node> { _node.successor };
            var current = _node.successor;

            // Get up to SUCCESSOR_LIST_SIZE - 1 additional successors
            for (int i = 1; i < Globals.SUCCESSOR_LIST_SIZE; i++)
            {
                try
                {
                    var nextSuccessor = await _getSuccessorService.ClientGet(current.ip);
                    if (nextSuccessor.ip == _node.ip || nextSuccessor.ip == current.ip)
                        break;

                    current = new M_Node { id = nextSuccessor.id, ip = nextSuccessor.ip };
                    newList.Add(current);
                }
                catch
                {
                    break;
                }
            }

            _node.successor_list = newList;
            return _node;
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(1, $"UpdateSuccessorList failed: {ex.Message}");
            return _node;
        }
    }

    // Add retry mechanism for RPC calls
    private static async Task<T> RetryRPC<T>(Func<Task<T>> rpc, int maxRetries = 3)
    {
        for (int i = 0; i < maxRetries; i++)
        {
            try
            {
                var result = await rpc();
                if (result == null && i < maxRetries - 1)
                {
                    await Task.Delay((i + 1) * 1000);
                    continue;
                }
                return result;
            }
            catch (Exception ex) when (i < maxRetries - 1)
            {
                await AgnetaHandler.Log(1, $"RPC attempt {i + 1} failed: {ex.Message}");
                await Task.Delay((i + 1) * 1000);
            }
        }
        return await rpc(); // Let the last attempt throw if it fails
    }


    // Improve Join method error handling
    public static async Task Join(M_Node _node, string bootstrapNodeIp)
    {
        if (_node == null)
        {
            throw new ArgumentNullException(nameof(_node), "Node cannot be null");
        }

        try
        {
            await AgnetaHandler.Log(1, $"Joining with id: {_node.id}");

            // Initialize the finger table if null
            if (_node.fingerTable == null)
            {
                _node.fingerTable = new Dictionary<ulong, M_Node>();
            }

            // Initialize successor list if null
            if (_node.successor_list == null)
            {
                _node.successor_list = new List<M_Node>();
            }

            if (string.IsNullOrEmpty(bootstrapNodeIp))
            {
                await InitializeFirstNode(_node);
                return;
            }

            await JoinExistingNetwork(_node, bootstrapNodeIp);
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(1, $"Join failed: {ex.Message}");
            throw;
        }
    }

    private static async Task InitializeFirstNode(M_Node _node)
    {
        try
        {
            await AgnetaHandler.Log(1, "Initializing first node in network");

            _node.successor = new M_Node { id = _node.id, ip = _node.ip };
            _node.predecessor = new M_Node { id = _node.id, ip = _node.ip };

            // Clear and initialize successor list
            _node.successor_list.Clear();
            _node.successor_list.Add(new M_Node { id = _node.id, ip = _node.ip });

            await InitFingerTable(_node);
            Globals._NODE = _node;

            await AgnetaHandler.Log(1, "First node initialization complete");
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(1, $"First node initialization failed: {ex.Message}");
            throw;
        }
    }

    private static async Task JoinExistingNetwork(M_Node _node, string bootstrapNodeIp)
    {
        try
        {
            // Find successor through bootstrap node
            var successor = await RetryRPC(async () =>
            {
                var queryRes = await _findPeerResponsible.ClientFind(
                    new QueryReq { Val = _node.id }, 
                    bootstrapNodeIp
                );

                if (queryRes?.Res == null)
                {
                    throw new Exception("Failed to find responsible peer");
                }

                var successorInfo = await _getNodeInfoService.ClientGet(queryRes.Res);
                if (successorInfo == null)
                {
                    throw new Exception("Failed to get node info for successor");
                }

                return new M_Node { id = successorInfo.Id, ip = successorInfo.Ip };
            });

            if (successor == null)
            {
                throw new Exception("Failed to get valid successor");
            }

            _node.successor = successor;
            _node.successor_list.Clear();
            _node.successor_list.Add(successor);

            // Update predecessor
            await UpdatePredecessorAndLinks(_node);

            // Build finger table and update others
            await InitFingerTable(_node);
            await UpdateOthers(_node);

            Globals._NODE = _node;

            await AgnetaHandler.Log(1, "Join existing network complete");
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(1, $"Join existing network failed: {ex.Message}");
            throw;
        }
    }

    private static async Task UpdatePredecessorAndLinks(M_Node _node)
    {
        try
        {
            // Get predecessor
            var predInfo = await RetryRPC(async () =>
                await _getPredecessorService.ClientGet(_node.successor.ip));

            if (predInfo == null)
            {
                throw new Exception("Failed to get predecessor info");
            }

            _node.predecessor = new M_Node { id = predInfo.Id, ip = predInfo.Ip };

            // Update links
            await RetryRPC(async () =>
            {
                await _updatePredecessorService.ClientUpdate(
                    new UpdatePredecessor_Req { Id = _node.id, Ip = _node.ip },
                    _node.successor.ip
                );
                return true;
            });

            await RetryRPC(async () =>
            {
                await _updateSuccessorService.ClientUpdate(
                    new UpdateSuccessor_Req { Id = _node.id, Ip = _node.ip },
                    _node.predecessor.ip
                );
                return true;
            });
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(1, $"Update predecessor and links failed: {ex.Message}");
            throw;
        }
    }

    private static async Task UpdateNodeLinks(M_Node _node)
    {
        await RetryRPC(async () =>
        {
            await _updatePredecessorService.ClientUpdate(
                new UpdatePredecessor_Req { Id = _node.id, Ip = _node.ip },
                _node.successor.ip
            );
            return true;
        });

        await RetryRPC(async () =>
        {
            await _updateSuccessorService.ClientUpdate(
                new UpdateSuccessor_Req { Id = _node.id, Ip = _node.ip },
                _node.predecessor.ip
            );
            return true;
        });
    }

    public static async Task<M_Node> InitFingerTable(M_Node _node)
    {
        ulong m = 1UL << Globals.FINGER_TABLE_SIZE;

        // Initialize or clear the finger table
        if (_node.fingerTable == null)
        {
            _node.fingerTable = new Dictionary<ulong, M_Node>();
        }
        else
        {
            _node.fingerTable.Clear();
        }

        // Initialize first finger (successor)
        ulong firstStart = (_node.id + 1) % m;
        _node.fingerTable[firstStart] = _node.successor;

        // Fill rest of finger table
        for (int i = 1; i < Globals.FINGER_TABLE_SIZE; i++)
        {
            try
            {
                ulong start = (_node.id + (1UL << i)) % m;

                // Initialize with successor first
                _node.fingerTable[start] = _node.successor;

                // If this is the first node, all fingers point to self
                if (_node.successor.ip == _node.ip)
                {
                    _node.fingerTable[start] = new M_Node { id = _node.id, ip = _node.ip };
                    continue;
                }

                // Otherwise, find the correct node for this finger
                string responsibleIp = await FindSuccessor(_node, start);
                if (responsibleIp != _node.ip)
                {
                    GetNodeInfo_Result nodeInfo = await _getNodeInfoService.ClientGet(responsibleIp);
                    _node.fingerTable[start] = new M_Node { 
                        id = nodeInfo.Id, 
                        ip = nodeInfo.Ip 
                    };
                }
                else
                {
                    _node.fingerTable[start] = new M_Node { id = _node.id, ip = _node.ip };
                }
            }
            catch (Exception ex)
            {
                await AgnetaHandler.Log(1, $"Failed to initialize finger {i}: {ex.Message}");
                // If we fail to get a specific finger, use the successor as fallback
                ulong start = (_node.id + (1UL << i)) % m;
                _node.fingerTable[start] = _node.successor;
            }
        }

        await AgnetaHandler.Log(1, $"Finger table initialized with {_node.fingerTable.Count} entries");
        return _node;
    }

    // Helper method to safely get finger table entry
    public static M_Node GetFingerTableEntry(M_Node _node, ulong start)
    {
        if (_node.fingerTable.TryGetValue(start, out M_Node finger))
        {
            return finger;
        }
        // Return successor as fallback if finger doesn't exist
        return _node.successor;
    }

    // Update ClosestPrecedingFinger to use safe finger table access
    private static M_Node ClosestPrecedingFinger(M_Node _node, ulong id)
    {
        ulong m = 1UL << Globals.FINGER_TABLE_SIZE;

        for (int i = Globals.FINGER_TABLE_SIZE - 1; i >= 0; i--)
        {
            ulong start = (_node.id + (1UL << i)) % m;
            M_Node finger = GetFingerTableEntry(_node, start);

            if (NodeUtils.inBetween(finger.id, _node.id, id))
            {
                return finger;
            }
        }
        return _node;
    }

    // Update BuildFingerTable method to be more robust
    public static async Task<M_Node> BuildFingerTable(M_Node _node)
    {
        ulong m = 1UL << Globals.FINGER_TABLE_SIZE;

        if (_node.fingerTable == null)
        {
            _node.fingerTable = new Dictionary<ulong, M_Node>();
        }

        // First finger is always the successor
        ulong firstStart = (_node.id + 1) % m;
        _node.fingerTable[firstStart] = _node.successor;

        for (int i = 1; i < Globals.FINGER_TABLE_SIZE; i++)
        {
            try
            {
                ulong fingerStart = (_node.id + (1UL << i)) % m;

                string _ip = await FindSuccessor(_node, fingerStart);
                if (_ip != _node.ip)
                {
                    GetNodeInfo_Result _getNodeInfo_Result = await _getNodeInfoService.ClientGet(_ip);
                    _node.fingerTable[fingerStart] = new M_Node { 
                        id = _getNodeInfo_Result.Id, 
                        ip = _getNodeInfo_Result.Ip 
                    };
                }
                else
                {
                    _node.fingerTable[fingerStart] = new M_Node { 
                        id = _node.id, 
                        ip = _node.ip 
                    };
                }
            }
            catch (Exception ex)
            {
                await AgnetaHandler.Log(1, $"Failed to build finger {i}: {ex.Message}");
                // Use successor as fallback
                ulong fingerStart = (_node.id + (1UL << i)) % m;
                _node.fingerTable[fingerStart] = _node.successor;
            }
        }

        if (_node.successor.ip != _node.ip)
        {
            await UpdateOthers(_node);
        }

        return _node;
    }

}
