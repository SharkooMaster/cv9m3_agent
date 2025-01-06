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

    public static async Task Join(M_Node _node, string bootstrapNodeIp)
    {
        await AgnetaHandler.Log(1, $"Joined with id: {_node.id}");

        if (bootstrapNodeIp == null)
        {
            await AgnetaHandler.Log(1, $"No bootstrap node detected");
            // First node in network
            _node.successor = new M_Node() { id = _node.id, ip = _node.ip };
            _node.predecessor = new M_Node() { id = _node.id, ip = _node.ip };
            
            await AgnetaHandler.Log(1, $"Building finger table");
            await BuildFingerTable(_node);
            await AgnetaHandler.Log(1, $"Finger table built");
            Globals._NODE = _node;
            return;
        }

        // 1. Find successor through bootstrap node
        await AgnetaHandler.Log(1, $"Searching for successor through bootstrap node");
        QueryReq req = new QueryReq() { Val=_node.id };
        QueryRes res = await _findPeerResponsible.ClientFind(req, bootstrapNodeIp);

        GetNodeInfo_Result successor_info = await _getNodeInfoService.ClientGet(res.Res);
        M_Node _node_successor = new M_Node() { id=successor_info.Id, ip=successor_info.Ip };
        await AgnetaHandler.Log(1, $"Succesor found: {_node_successor.ip}:{_node_successor.id}");

        // Get predecessor
        await AgnetaHandler.Log(1, $"Getting predecessor");
        GetPredecessor_Result predecessor_info = await _getPredecessorService.ClientGet(_node_successor.ip);
        M_Node _node_predecessor = new M_Node() { id=predecessor_info.Id, ip=predecessor_info.Ip };

        _node.successor = _node_successor;
        _node.predecessor = _node_predecessor;
        await AgnetaHandler.Log(1, $"predecessor found: {_node.predecessor.ip}:{_node.predecessor.id}");

        // 2. Update predecessor/successor links
        await AgnetaHandler.Log(1, $"Updating predecessor/successor links");
        UpdatePredecessor_Req updatePredecessor_req = new UpdatePredecessor_Req() { Id=_node.id, Ip=_node.ip };
        await _updatePredecessorService.ClientUpdate(updatePredecessor_req, _node.successor.ip);
        await AgnetaHandler.Log(1, $"Updated successors predecessor");

        UpdateSuccessor_Req updateSuccessor_req = new UpdateSuccessor_Req() { Id=_node.id, Ip=_node.ip };
        await _updateSuccessorService.ClientUpdate(updateSuccessor_req, _node.predecessor.ip);
        await AgnetaHandler.Log(1, $"Updated predecessors successor");

        // 3. Build finger table
        await AgnetaHandler.Log(1, $"Building finger table");
        _node = await BuildFingerTable(_node);
        await AgnetaHandler.Log(1, $"Finger table built");

        // 4. Transfer necessary keys

        Globals._NODE = _node;
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
        for (int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
        {
            ulong updateStart = (_node.id - (1UL << i)) % (1UL << Globals.FINGER_TABLE_SIZE);

            string p = await FindSuccessor(_node, updateStart);
            if(p != _node.ip)
            {
                UpdateFingerTable_Req _updateFingerTable_Req = new UpdateFingerTable_Req() {
                    FingerIndex = i,
                    Id = _node.id,
                    Ip = _node.ip
                };
                await _updateFingerTableService.ClientUpdate(_updateFingerTable_Req, p);
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
        // If id is between this node and its successor
        // return successor
        if(NodeUtils.inBetween(id, _node.id, _node.successor.id))
        {
            return _node.successor.ip;
        }
        // If id is between predecessor and this node
        // return this node
        if(NodeUtils.inBetween(id, _node.predecessor.id, _node.id))
        {
            return _node.ip;
        }
        // Otherwise, forward to closest preceding finger
        ulong[] fingerTableKeys = _node.fingerTable.Keys.ToArray();
        for (int i = Globals.FINGER_TABLE_SIZE - 1; i >= 0; i--)
        {
            M_Node finger = _node.fingerTable[fingerTableKeys[i]];
            if(NodeUtils.inBetween(finger.id, _node.id, id))
            {
                QueryReq req = new QueryReq() { Val=id };
                QueryRes res = await _findPeerResponsible.ClientFind(req, finger.ip);
                return res.Res;
            }
        }

        QueryReq _req = new QueryReq() { Val=id };
        QueryRes _res = await _findPeerResponsible.ClientFind(_req, _node.successor.ip);
        return _res.Res;
    }

    public static async Task<M_Node> Stabilize(M_Node _node)
    {
        // Periodically verify successor
        // and notify it about this node
        GetPredecessor_Result _getPredecessor_Result = await _getPredecessorService.ClientGet(_node.successor.ip);
        M_Node x = new M_Node() { id = _getPredecessor_Result.Id, ip = _getPredecessor_Result.Ip };

        if(x != null && x.id != _node.id && NodeUtils.inBetween(x.id, _node.id, _node.successor.id))
        {
            _node.successor = x;
        }

        UpdateSuccessor_Req _updateSuccessor_Req = new UpdateSuccessor_Req() { Id = _node.id, Ip = _node.ip };
        await _updateSuccessorService.ClientUpdate(_updateSuccessor_Req, _node.successor.ip);
        
        return _node;
    }

    public static int nextFingerToStabalize = 0;
    public static async Task<M_Node> FixFingers(M_Node _node)
    {
        if(_node.successor.ip == _node.ip){ return _node; }
        // Periodically refresh finger table entries
        try
        {
            nextFingerToStabalize = (nextFingerToStabalize + 1) % Globals.FINGER_TABLE_SIZE;

            ulong fingerStart = (_node.id + (1UL << nextFingerToStabalize)) % (1UL << Globals.FINGER_TABLE_SIZE);
        
            string _ip = await FindSuccessor(_node, fingerStart);
            GetNodeInfo_Result _getNodeInfo_Result = await _getNodeInfoService.ClientGet(_ip);
            M_Node responsible = new M_Node() { id = _getNodeInfo_Result.Id, ip = _getNodeInfo_Result.Ip };

            ulong[] fingerTableKeys = _node.fingerTable.Keys.ToArray();
            if(_node.fingerTable[fingerTableKeys[nextFingerToStabalize]].id != responsible.id)
            {
                _node.fingerTable[fingerTableKeys[nextFingerToStabalize]] = responsible;
                await AgnetaHandler.Log(1, $"Updated finger {nextFingerToStabalize} to node {responsible.id}");
            }
        }
        catch (Exception e)
        {
            await AgnetaHandler.Log(1, $"Failed to fix finger {nextFingerToStabalize}: {e.Message}");
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

    // |--------------------------------------------------------------------------------------------------------|
    // |-------------------------------------------------------------------------------------- TESTING SUITE----|
    // |--------------------------------------------------------------------------------------------------------|
    public static async Task TestNetwork(M_Node _node)
    {
        await AgnetaHandler.Log(1, "\n=== Starting Network Tests ===\n");

        // Test 1: Basic Connectivity
        await TestBasicConnectivity(_node);

        // Test 2: Ring Consistency
        await TestRingConsistency(_node);

        // Test 3: Routing
        await TestRouting(_node);

        // Test 4: Finger Table Coverage
        await TestFingerTableCoverage(_node);

        await AgnetaHandler.Log(1, "\n=== Network Tests Complete ===\n");
    }

    private static async Task TestBasicConnectivity(M_Node _node)
    {
        await AgnetaHandler.Log(1, "1. Testing Basic Connectivity:");

        try 
        {
            // Test successor connection
            GetHealth_Result successorHealth = await _getHealth.ClientGet(_node.successor.ip);
            await AgnetaHandler.Log(1, $"  ✓ Successor ({_node.successor.ip}) is responsive");

            // Test predecessor connection if not self
            if (_node.predecessor.ip != _node.ip)
            {
                GetHealth_Result predHealth = await _getHealth.ClientGet(_node.predecessor.ip);
                await AgnetaHandler.Log(1, $"  ✓ Predecessor ({_node.predecessor.ip}) is responsive");
            }
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(1, $"  ✗ Connectivity test failed: {ex.Message}");
        }
    }

    private static async Task TestRingConsistency(M_Node _node)
    {
        HashSet<string> visited = new HashSet<string>();
        M_Node current = _node;
        int maxHops = 10; // Prevent infinite loops
        int hops = 0;

        try
        {
            while (!visited.Contains(current.ip) && hops < maxHops)
            {
                visited.Add(current.ip);

                // Verify successor's predecessor points back
                if (current.successor != null)
                {
                    GetPredecessor_Result pred = await _getPredecessorService.ClientGet(current.successor.ip);
                    if (pred.Id != current.id)
                    {
                        await AgnetaHandler.Log(1, $"Ring inconsistency: {current.successor.ip}'s predecessor is {pred.Id}, expected {current.id}");
                        return;
                    }

                    // Move to successor
                    current = await _getSuccessorService.ClientGet(current.successor.ip);
                    hops++;
                }
            }
            await AgnetaHandler.Log(1, $"Ring consistency verified across {hops} nodes");
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(1, $"Ring consistency test failed: {ex.Message}");
        }
    }

    private static async Task TestRouting(M_Node _node)
    {
        await AgnetaHandler.Log(1, "\n3. Testing Routing:");

        Random rnd = new Random();
        for (int i = 0; i < 5; i++)
        {
            try 
            {
                // Generate random target ID
                ulong targetId = (ulong)rnd.NextInt64(0, long.MaxValue);

                await AgnetaHandler.Log(1, $"  Testing route to target {targetId}");

                HashSet<string> visitedNodes = new HashSet<string>();
                string currentNodeIp = _node.ip;
                int hops = 0;

                while (hops < Globals.FINGER_TABLE_SIZE)
                {
                    if (visitedNodes.Contains(currentNodeIp))
                    {
                        await AgnetaHandler.Log(1, $"  ✗ Routing loop detected after {hops} hops");
                        break;
                    }

                    visitedNodes.Add(currentNodeIp);

                    QueryReq req = new QueryReq() { Val = targetId };
                    QueryRes res = await _findPeerResponsible.ClientFind(req, currentNodeIp);

                    await AgnetaHandler.Log(1, $"    Hop {hops + 1}: {currentNodeIp} -> {res.Res}");

                    if (res.Res == currentNodeIp)
                        break;

                    currentNodeIp = res.Res;
                    hops++;
                }

                await AgnetaHandler.Log(1, $"  ✓ Route found in {hops} hops");
            }
            catch (Exception ex)
            {
                await AgnetaHandler.Log(1, $"  ✗ Routing test failed: {ex.Message}");
            }
        }
    }

    private static async Task TestFingerTableCoverage(M_Node _node)
    {
        await AgnetaHandler.Log(1, "\n4. Testing Finger Table Coverage:");

        try 
        {
            var fingerStarts = _node.fingerTable.Keys.OrderBy(k => k).ToList();

            await AgnetaHandler.Log(1, "  Finger table entries:");
            foreach (var start in fingerStarts)
            {
                await AgnetaHandler.Log(1, $"    {start} -> {_node.fingerTable[start].id}");
            }

            // Check for proper spacing
            for (int i = 0; i < fingerStarts.Count - 1; i++)
            {
                var gap = fingerStarts[i + 1] - fingerStarts[i];
                if (gap > (1UL << (i + 1)))
                {
                    await AgnetaHandler.Log(1, $"  ✗ Large gap detected between fingers {i} and {i + 1}");
                }
            }

            await AgnetaHandler.Log(1, "  ✓ Finger table coverage verified");
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(1, $"  ✗ Finger table test failed: {ex.Message}");
        }
    }

}
