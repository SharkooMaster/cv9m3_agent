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

    public static async Task<M_Node> InitFingerTable(M_Node _node)
    {
        ulong m = 1UL << Globals.FINGER_TABLE_SIZE;
        _node.fingerTable.Clear();
        _node.fingerTable[(_node.id + 1) % m] = _node.successor;

        // Fill rest of finger table
        for (int i = 1; i < Globals.FINGER_TABLE_SIZE; i++)
        {
            ulong start = (_node.id + (1UL << i)) % m;

            // Check if we can use previous finger
            ulong prevStart = (_node.id + (1UL << (i-1))) % m;

            if (NodeUtils.inBetween(start, _node.id, _node.fingerTable[prevStart].id))
            {
                _node.fingerTable[start] = _node.fingerTable[prevStart];
            }
            else
            {
                string responsibleIp = await FindSuccessor(_node, start);
                GetNodeInfo_Result nodeInfo = await _getNodeInfoService.ClientGet(responsibleIp);
                _node.fingerTable[start] = new M_Node() { 
                    id = nodeInfo.Id, 
                    ip = nodeInfo.Ip 
                };
            }
        }

        return _node;
    }

    public static async Task<M_Node> BuildFingerTable(M_Node _node)
    {
        _node.fingerTable[(_node.id + (1UL << 0)) % (1UL << Globals.FINGER_TABLE_SIZE)] = _node.successor;

        for (int i = 1; i < Globals.FINGER_TABLE_SIZE; i++)
        {
            ulong fingerStart = (_node.id + (1UL << i)) % (1UL << Globals.FINGER_TABLE_SIZE);

            string _ip = await FindSuccessor(_node, fingerStart);
            if(_ip != _node.ip)
            {
                GetNodeInfo_Result _getNodeInfo_Result = await _getNodeInfoService.ClientGet(_ip);
                _node.fingerTable[fingerStart] = new M_Node() { id = _getNodeInfo_Result.Id, ip = _getNodeInfo_Result.Ip };
            }
            else
            {
                _node.fingerTable[fingerStart] = new M_Node() { id = _node.id, ip = _node.ip };
            }
        }

        if(_node.successor.ip != _node.ip)
        {
            await UpdateOthers(_node);
        }
        return _node;
    }

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

    private static M_Node ClosestPrecedingFinger(M_Node _node, ulong id)
    {
        for (int i = Globals.FINGER_TABLE_SIZE - 1; i >= 0; i--)
        {
            var finger = _node.fingerTable[(_node.id + (1UL << i)) % (1UL << Globals.FINGER_TABLE_SIZE)];
            if (NodeUtils.inBetween(finger.id, _node.id, id))
                return finger;
        }
        return _node;
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
        if (_node.successor.ip == _node.ip)
            return _node.ip;

        M_Node current = new M_Node { id = _node.id, ip = _node.ip };
        M_Node successor = _node.successor;

        while (!NodeUtils.inBetween(id, current.id, successor.id) && id != successor.id)
        {
            try
            {
                var n = ClosestPrecedingFinger(current, id);
                if (n.ip == current.ip)
                    break;

                current = n;
                M_Node succRes = await _getSuccessorService.ClientGet(current.ip);
                successor = succRes;
            }
            catch (Exception ex)
            {
                await AgnetaHandler.Log(1, $"FindPredecessor failed: {ex.Message}");
                throw;
            }
        }

        return current.ip;
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
                return await rpc();
            }
            catch (Exception ex) when (i < maxRetries - 1)
            {
                await AgnetaHandler.Log(1, $"RPC attempt {i + 1} failed: {ex.Message}");
                await Task.Delay((i + 1) * 1000); // Exponential backoff
            }
        }
        return await rpc(); // Let the last attempt throw if it fails
    }

    // Improve Join method error handling
    public static async Task Join(M_Node _node, string bootstrapNodeIp)
    {
        try
        {
            await AgnetaHandler.Log(1, $"Joining with id: {_node.id}");

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
        await AgnetaHandler.Log(1, "Initializing first node in network");
        _node.successor = new M_Node { id = _node.id, ip = _node.ip };
        _node.predecessor = new M_Node { id = _node.id, ip = _node.ip };
        _node.successor_list = new List<M_Node> { new M_Node { id = _node.id, ip = _node.ip } };

        await InitFingerTable(_node);
        Globals._NODE = _node;
    }

    private static async Task JoinExistingNetwork(M_Node _node, string bootstrapNodeIp)
    {
        // Find successor through bootstrap node
        var successor = await RetryRPC(async () =>
        {
            var res = await _findPeerResponsible.ClientFind(new QueryReq { Val = _node.id }, bootstrapNodeIp);
            var successorInfo = await _getNodeInfoService.ClientGet(res.Res);
            return new M_Node { id = successorInfo.Id, ip = successorInfo.Ip };
        });

        _node.successor = successor;
        _node.successor_list.Add(successor);

        // Get and update predecessor
        var predecessor = await RetryRPC(async () =>
        {
            var predInfo = await _getPredecessorService.ClientGet(successor.ip);
            return new M_Node { id = predInfo.Id, ip = predInfo.Ip };
        });

        _node.predecessor = predecessor;

        // Update links first, then build finger table
        await UpdateNodeLinks(_node);
        await InitFingerTable(_node);
        await UpdateOthers(_node);

        Globals._NODE = _node;
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

}
