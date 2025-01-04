using Agent.Interfaces;
using Agent.Models;
using Agent.Modules.Agneta;
using Agent.Utils;
using Agent.Utils.Globals;

namespace Agent.Modules.Peer;

public static class NodeService
{
    public static async Task BuildFingerTable(M_Node _node)
    {
        /*
            * A position in a finger table for i < m : ((id + 2^i) % 2^m) (m = hashring max).
            * Use the dht to look up each one of those keys.
        */
        for (int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
        {
            ulong shift = 1UL << i;
            ulong sum = _node.id + shift;
            ulong modulo = (1UL << Globals.FINGER_TABLE_SIZE);
            ulong fingerStart = sum % modulo;

            Console.WriteLine($"bootstrap building, sending find peer request, target ip: {_node.successor.ip}, my ip: {_node.ip}");
            FindPeerResponsibleService fprs = new FindPeerResponsibleService();
            QueryReq req = new QueryReq() { Val = fingerStart };
            QueryRes res = await fprs.ClientFind(req, _node.successor.ip);

            Console.WriteLine($"Retrieved peer from successor: {res.Res}, sending getNodeInfo request");
            GetNodeInfo_Result gnis_res;
            if(res.Res != _node.ip)
            {
                GetNodeInfoService gnis = new GetNodeInfoService();
                gnis_res = await gnis.ClientGet(res.Res);
                Console.WriteLine($"Node info retrieved: {gnis_res.Ip}");
            }
            else
            {
                gnis_res = new GetNodeInfo_Result();
                gnis_res.Id = _node.id;
                gnis_res.Ip = _node.ip;
            }

            if (!_node.fingerTable.ContainsKey(fingerStart))
            {
                _node.fingerTable.Add(fingerStart, new M_Node()
                {
                    id = gnis_res.Id,
                    ip = gnis_res.Ip
                });
            }
        }

        Globals._NODE = _node;
    }

    public static async Task<M_Node> UpdateFingerTable(M_Node _node, M_Node new_node, int finger_index)
    {
        // ulong[] fingerTableKeys = _node.fingerTable.Keys.ToArray();
        ulong shift = 1UL << finger_index;
        ulong sum = _node.id + shift;
        ulong modulo = (1UL << Globals.FINGER_TABLE_SIZE);
        ulong start = sum % modulo;

        ulong[] fingerTableKeys = _node.fingerTable.Keys.ToArray();
        if(NodeUtils.inBetween(new_node.id, _node.id, _node.fingerTable[fingerTableKeys[finger_index]].id))
        {
            _node.fingerTable[fingerTableKeys[finger_index]] = new_node;

            if(_node.predecessor != null && _node.predecessor.id != _node.id)
            {
                UpdateFingerTableService ufts = new UpdateFingerTableService();
                UpdateFingerTable_Req ufts_req = new UpdateFingerTable_Req() { FingerIndex = finger_index, Id = new_node.id, Ip = new_node.ip };
                await ufts.ClientUpdate(ufts_req, _node.predecessor.ip);
            }
        }
        return _node;
    }

    public static async Task UpdateOthers(M_Node _node)
    {
        for (int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
        {
            // Find last node p whose i-th finger might be us
            ulong update_start = (_node.id - (1UL << i)) % (1UL << Globals.FINGER_TABLE_SIZE);
            
            // Find the node responsible for update_start
            FindPeerResponsibleService fprs = new FindPeerResponsibleService();
            QueryReq req = new QueryReq() { Val = update_start };
            QueryRes res = await fprs.ClientFind(req, _node.successor.ip);
            
            // Update that node's finger table
            if (res.Res != _node.ip)  // Don't update ourselves
            {
                UpdateFingerTableService ufts = new UpdateFingerTableService();
                UpdateFingerTable_Req ufts_req = new UpdateFingerTable_Req() 
                { 
                    FingerIndex = i, 
                    Id = _node.id, 
                    Ip = _node.ip 
                };
                await ufts.ClientUpdate(ufts_req, res.Res);
            }
        }
    }

    public static async void PrintNodeState(M_Node node)
    {
        await AgnetaHandler.Log(1, $"\n=== Node State (ID: {node.id}) ===");
        await AgnetaHandler.Log(1, $"IP: {node.ip}");
        await AgnetaHandler.Log(1, $"Predecessor: {node.predecessor.id} ({node.predecessor.ip})");
        await AgnetaHandler.Log(1, $"Successor: {node.successor.id} ({node.successor.ip})");
        await AgnetaHandler.Log(1, "\nFinger Table:");
        await AgnetaHandler.Log(1, "Start\t\tNode ID\t\tNode IP");
        await AgnetaHandler.Log(1, "----------------------------------------");

        Console.WriteLine($"\n=== Node State (ID: {node.id}) ===");
        Console.WriteLine($"IP: {node.ip}");
        Console.WriteLine($"Predecessor: {node.predecessor.id} ({node.predecessor.ip})");
        Console.WriteLine($"Successor: {node.successor.id} ({node.successor.ip})");
    
        Console.WriteLine("\nFinger Table:");
        Console.WriteLine("Start\t\tNode ID\t\tNode IP");
        Console.WriteLine("----------------------------------------");
        foreach (var entry in node.fingerTable.OrderBy(x => x.Key))
        {
            // Console.WriteLine($"{entry.Key}\t\t{entry.Value.id}\t\t{entry.Value.ip}");
            await AgnetaHandler.Log(1, $"{entry.Key}\t\t{entry.Value.id}\t\t{entry.Value.ip}");
        }
        Console.WriteLine("========================================\n");
        await AgnetaHandler.Log(1, "========================================\n");
    }

    public static async Task TestRouting(M_Node startNode, ulong targetId, HashSet<string> visitedNodes = null)
    {
        if (visitedNodes == null)
            visitedNodes = new HashSet<string>();

        Console.WriteLine($"Testing route from Node {startNode.id} to target {targetId}");
        await AgnetaHandler.Log(1, $"Testing route from Node {startNode.id} to target {targetId}");

        var hops = 0;
        var currentNodeIp = startNode.ip;
        FindPeerResponsibleService fprs = new FindPeerResponsibleService();

        while (hops < Globals.FINGER_TABLE_SIZE) // Shouldn't take more hops than finger table size
        {
            if (visitedNodes.Contains(currentNodeIp))
            {
                Console.WriteLine("ERROR: Routing loop detected!");
                await AgnetaHandler.Log(1, "ERROR: Routing loop detected!");
                return;
            }

            visitedNodes.Add(currentNodeIp);
            
            string res = "";
            await AgnetaHandler.Log(1, $"Testing rout to ip: {currentNodeIp}, my ip: {Globals._NODE.ip}");
            if(currentNodeIp != Globals._NODE.ip)
            {
                QueryReq req = new QueryReq() { Val = targetId };
                QueryRes _res = await fprs.ClientFind(req, currentNodeIp);
                res = _res.Res;
            }
            else
            {
                res = await FindPeerResponsible(Globals._NODE, targetId);
            }

            // Console.WriteLine($"Hop {hops + 1}: Node {currentNodeIp} -> {res.Res}");
            await AgnetaHandler.Log(1, $"Hop {hops + 1}: Node {currentNodeIp} -> {res}");

            if (res == currentNodeIp)
                break;

            currentNodeIp = res;
            hops++;
        }

        Console.WriteLine($"Route found in {hops} hops");
        await AgnetaHandler.Log(1, $"Route found in {hops} hops");
        if (hops >= Math.Log2(Globals.FINGER_TABLE_SIZE))
        {
            Console.WriteLine("WARNING: Route took more hops than expected for efficient routing");
            await AgnetaHandler.Log(1, "WARNING: Route took more hops than expected for efficient routing");
        }
    }

    public static async Task TestDHT(M_Node node)
    {
        // Test 1: Basic connectivity
        Console.WriteLine("=== Testing Basic Connectivity ===");
        await AgnetaHandler.Log(1, "=== Testing Basic Connectivity ===");
        PrintNodeState(node);

        // Test 2: Successor/Predecessor consistency
        Console.WriteLine("=== Testing Successor/Predecessor Links ===");
        await AgnetaHandler.Log(1, "=== Testing Successor/Predecessor Links ===");
        var current = node;
        var visited = new HashSet<string>();
        var count = 0;

        GetPredecessorService gps = new GetPredecessorService();
        GetSuccessorService gss = new GetSuccessorService();
        while (!visited.Contains(current.ip) && count < 100)
        {
            visited.Add(current.ip);
            // Console.WriteLine($"Node {current.id} -> Successor {current.successor.id}");
            await AgnetaHandler.Log(1, $"Node {current.id} -> Successor {current.successor.id} : {current.successor.ip}");

            // Verify that this node is its successor's predecessor
            if(current.successor.ip == node.ip){ break; }
            GetPredecessor_Result pred = await gps.ClientGet(current.successor.ip);
            await AgnetaHandler.Log(1, "Predecessor recieved");
            if (pred.Id != current.id)
            {
                Console.WriteLine($"ERROR: Node {current.successor.id}'s predecessor is {pred.Id}, expected {current.id}");
                await AgnetaHandler.Log(1, $"ERROR: Node {current.successor.id}'s predecessor is {pred.Id}, expected {current.id}");
            }

            current = current.successor;
            M_Node gss_res = await gss.ClientGet(current.ip);
            current.successor = gss_res;
            count++;
            await AgnetaHandler.Log(1, "Count updated");
        }

        // Test 3: Routing efficiency
        Console.WriteLine("=== Testing Routing Efficiency ===");
        await AgnetaHandler.Log(1, "=== Testing Routing Efficiency ===");
        // Test a few random targets
        Random rnd = new Random();
        for (int i = 0; i < 5; i++)
        {
            // Use the same modulo we use elsewhere in the Chord implementation
            ulong modulo = 1UL << Globals.FINGER_TABLE_SIZE;
            // Generate random ulong within our ring size
            ulong targetId = (ulong)rnd.NextInt64(0, long.MaxValue) % modulo;
            await TestRouting(node, targetId);
        }

        // Test 4: Finger table coverage
        Console.WriteLine("\n=== Testing Finger Table Coverage ===");
        await AgnetaHandler.Log(1, "\n=== Testing Finger Table Coverage ===");
        var fingerStarts = node.fingerTable.Keys.OrderBy(k => k).ToList();
        for (int i = 0; i < fingerStarts.Count - 1; i++)
        {
            var gap = fingerStarts[i + 1] - fingerStarts[i];
            if (gap > (1UL << (i + 1)))
            {
                Console.WriteLine($"WARNING: Large gap between finger {i} ({fingerStarts[i]}) and {i + 1} ({fingerStarts[i + 1]})");
                await AgnetaHandler.Log(1, $"WARNING: Large gap between finger {i} ({fingerStarts[i]}) and {i + 1} ({fingerStarts[i + 1]})");
            }
        }
        await AgnetaHandler.Log(1, "\n=== Complete ===");
    }

    public static async Task JoinNetwork(M_Node _node, string bootstrap_node_ip)
    {
        _node.id = NodeUtils.generateNodeID();
        await AgnetaHandler.Log(1, $"[DEBUG] Starting JoinNetwork - Node ID: {_node.id}, Bootstrap IP: {bootstrap_node_ip}");

        if(bootstrap_node_ip == _node.ip || bootstrap_node_ip == null || bootstrap_node_ip == "")
        {
            await AgnetaHandler.Log(1, "[DEBUG] Initializing as first node in network");
            // Only peer in the network
            _node.predecessor = _node;
            _node.successor = _node;

            for(int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
            {
                ulong shift = 1UL << i;
                ulong sum = _node.id + shift;
                ulong modulo = (1UL << Globals.FINGER_TABLE_SIZE);
                ulong fingerStart = sum % modulo;

                await AgnetaHandler.Log(1, $"[DEBUG] Adding finger table entry {i}: Start={fingerStart}");

                if (!_node.fingerTable.ContainsKey(fingerStart))
                {
                    _node.fingerTable.Add(fingerStart, _node);
                }
            }
        }
        else
        {
            await AgnetaHandler.Log(1, "[DEBUG] Joining existing network");

            // Find successor
            FindPeerResponsibleService fprs = new FindPeerResponsibleService();
            QueryReq req = new QueryReq() { Val=_node.id };
            QueryRes result = await fprs.ClientFind(req, bootstrap_node_ip);
            string _successor_ip = result.Res;

            await AgnetaHandler.Log(1, $"[DEBUG] Found successor IP: {_successor_ip}");

            // Get successor info
            GetNodeInfoService gnis = new GetNodeInfoService();
            GetNodeInfo_Result gnis_res = await gnis.ClientGet(_successor_ip);

            M_Node _successor = new M_Node 
            { 
                id = gnis_res.Id,
                ip = _successor_ip
            };

            await AgnetaHandler.Log(1, $"[DEBUG] Successor details - ID: {_successor.id}, IP: {_successor.ip}");

            _node.successor = _successor;
            _successor.predecessor = _node;
            
            // Update the predecessor's successor link
            UpdateSuccessorService uss = new UpdateSuccessorService();
            if (_node.predecessor != null && _node.predecessor.id != _node.id)
            {
                await uss.ClientUpdate(new UpdateSuccessor_Req() 
                { 
                    Id = _node.id, 
                    Ip = _node.ip 
                }, _node.predecessor.ip);
            }

            // Set relationships
            _node.successor = _successor;

            // Initialize finger table
            await AgnetaHandler.Log(1, "[DEBUG] Starting finger table initialization");
            _node = await InitFingerTable(_node);

            // Get and set predecessor
            GetPredecessorService gps = new GetPredecessorService();
            GetPredecessor_Result gps_res = await gps.ClientGet(_successor.ip);

            M_Node _predecessor = new M_Node() 
            { 
                id = gps_res.Id, 
                ip = gps_res.Ip 
            };
            _node.predecessor = _predecessor;

            await AgnetaHandler.Log(1, $"[DEBUG] Predecessor set - ID: {_predecessor.id}, IP: {_predecessor.ip}");

            // Update other nodes
            await AgnetaHandler.Log(1, "[DEBUG] Updating predecessor and successor links");

            // Update successor's predecessor
            UpdatePredecessorService ups = new UpdatePredecessorService();
            await ups.ClientUpdate(new UpdatePredecessor_Req(){ Id = _node.id, Ip = _node.ip }, _successor.ip);

            // Update predecessor's successor
            await uss.ClientUpdate(new UpdateSuccessor_Req(){ Id = _node.id, Ip = _node.ip }, _predecessor.ip);

            await AgnetaHandler.Log(1, "[DEBUG] Starting UpdateOthers");
            await UpdateOthers(_node);

            await AgnetaHandler.Log(1, "[DEBUG] Network join complete - Running DHT test");
            await TestDHT(_node);
        }

        Globals._NODE = _node;
    }

    public static async Task<M_Node> InitFingerTable(M_Node _node)
    {
        // First finger is successor
        ulong firstFingerStart = (_node.id + 1) % (1UL << Globals.FINGER_TABLE_SIZE);
        _node.fingerTable[firstFingerStart] = _node.successor;
    
        // For remaining fingers
        for (int i = 1; i < Globals.FINGER_TABLE_SIZE; i++)
        {
            ulong fingerStart = (_node.id + (1UL << i)) % (1UL << Globals.FINGER_TABLE_SIZE);
            
            // Only reuse previous finger if really in the same interval
            if (i > 0)
            {
                ulong prevStart = (_node.id + (1UL << (i-1))) % (1UL << Globals.FINGER_TABLE_SIZE);
                ulong prevNode = _node.fingerTable[prevStart].id;
                
                // Check if this finger should use a different node
                if (!NodeUtils.inBetween(fingerStart, prevNode, _node.fingerTable[prevStart].id))
                {
                    // Find new responsible node
                    FindPeerResponsibleService fprs = new FindPeerResponsibleService();
                    QueryReq req = new QueryReq() { Val = fingerStart };
                    QueryRes res = await fprs.ClientFind(req, _node.successor.ip);
    
                    GetNodeInfoService gnis = new GetNodeInfoService();
                    GetNodeInfo_Result nodeInfo = await gnis.ClientGet(res.Res);
    
                    _node.fingerTable[fingerStart] = new M_Node() 
                    { 
                        id = nodeInfo.Id, 
                        ip = nodeInfo.Ip 
                    };
                }
                else
                {
                    _node.fingerTable[fingerStart] = _node.fingerTable[prevStart];
                }
            }
        }
        return _node;
    }

    public static async Task<string> FindPeerResponsible(M_Node _node, ulong target)
    {
        // If we're alone in the network
        if (_node.successor.id == _node.id)
            return _node.ip;

        // If target is between us and our successor
        if (NodeUtils.inBetween(target, _node.id, _node.successor.id))
            return _node.successor.ip;

        // If target is between predecessor and us
        if (NodeUtils.inBetween(target, _node.predecessor.id, _node.id))
            return _node.ip;

        // Find closest preceding finger
        M_Node nextHop = ClosestPrecedingFinger(_node, target);

        // If we're the closest, return successor
        if (nextHop.id == _node.id)
            return _node.successor.ip;

        return nextHop.ip;
    }

    public static M_Node ClosestPrecedingFinger(M_Node _node, ulong target)
    {
        // Look through finger table from farthest to closest
        ulong[] fingerTableKeys = _node.fingerTable.Keys.ToArray();
        for (int i = Globals.FINGER_TABLE_SIZE - 1; i >= 0; i--)
        {
            M_Node finger = _node.fingerTable[fingerTableKeys[i]];
            // Check if this finger is between us and the target
            if (NodeUtils.inBetween(finger.id, _node.id, target))
            {
                return finger;
            }
        }
        return _node;
    }
}
