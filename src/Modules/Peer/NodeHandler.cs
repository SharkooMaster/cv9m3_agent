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

    public static async Task<M_Node> InitFingerTable(M_Node _node)
    {
        // First finger is just our successor
        ulong firstFingerStart = (_node.id + 1) % (1UL << Globals.FINGER_TABLE_SIZE);
        _node.fingerTable[firstFingerStart] = _node.successor;

        // For each remaining finger
        for (int i = 1; i < Globals.FINGER_TABLE_SIZE; i++)
        {
            ulong fingerStart = (_node.id + (1UL << i)) % (1UL << Globals.FINGER_TABLE_SIZE);

            // If this finger start is between us and our previous finger
            ulong prevFingerStart = (_node.id + (1UL << (i-1))) % (1UL << Globals.FINGER_TABLE_SIZE);
            if (NodeUtils.inBetween(fingerStart, _node.id, _node.fingerTable[prevFingerStart].id))
            {
                // We can use the same node
                _node.fingerTable[fingerStart] = _node.fingerTable[prevFingerStart];
            }
            else
            {
                // We need to find the responsible node through the network
                FindPeerResponsibleService fprs = new FindPeerResponsibleService();
                QueryReq req = new QueryReq() { Val = fingerStart };
                // Important: Start the search from our known successor
                QueryRes res = await fprs.ClientFind(req, _node.successor.ip);

                GetNodeInfoService gnis = new GetNodeInfoService();
                GetNodeInfo_Result nodeInfo = await gnis.ClientGet(res.Res);

                _node.fingerTable[fingerStart] = new M_Node() 
                { 
                    id = nodeInfo.Id, 
                    ip = nodeInfo.Ip 
                };
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

            // Find the predecessor of this position
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
    }

    public static async Task JoinNetwork(M_Node _node, string bootstrap_node_ip)
    {
        /*
            * Assign ID to this node    [x]
            * Find successor node through bootstrap_node [x]
            * Get predecessor node from successor and update there routings [x]
            * Build finger table [x]
        */
        _node.id = NodeUtils.generateNodeID();
        Console.WriteLine($"Joining network, id created: {_node.id.ToString()}");
        await AgnetaHandler.Log(1, $"Joining network, id created: {_node.id.ToString()}");

        if(bootstrap_node_ip == _node.ip || bootstrap_node_ip == null || bootstrap_node_ip == "")
        {
            Console.WriteLine($"Only peer in the network, creating default finger table.");
            await AgnetaHandler.Log(1, $"Only peer in the network, creating default finger table.");
            // Only peer in the network
            _node.predecessor = _node;
            _node.successor = _node;

            for(int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
            {
                // ulong fingerStart = (_node.id + (1UL << i)) % (1UL << Globals.FINGER_TABLE_SIZE);
                ulong shift = 1UL << i;
                ulong sum = _node.id + shift;
                ulong modulo = (1UL << Globals.FINGER_TABLE_SIZE);
                ulong fingerStart = sum % modulo;

                if (!_node.fingerTable.ContainsKey(fingerStart))
                {
                    _node.fingerTable.Add(fingerStart, _node);
                }
            }
            Console.WriteLine($"fingerTable length: {_node.fingerTable.Count}");
        }
        else
        {
            Console.WriteLine($"Bootstrap node exists");
            await AgnetaHandler.Log(1, $"Bootstrap node exists");
            // Use successor to create finger table
            // First get basic info of successor
            M_Node _successor = new M_Node();

            FindPeerResponsibleService fprs = new FindPeerResponsibleService();
            QueryReq req = new QueryReq() { Val=_node.id };
            QueryRes result = await fprs.ClientFind(req, bootstrap_node_ip);
            string _successor_ip = result.Res;
            _successor.ip = _successor_ip;
            Console.WriteLine($"Found successor with ip: {_successor.ip}");
            await AgnetaHandler.Log(1, $"Found successor with ip: {_successor.ip}");

            GetNodeInfoService gnis = new GetNodeInfoService();
            GetNodeInfo_Result gnis_res = await gnis.ClientGet(_successor_ip);

            _successor.id = gnis_res.Id;
            Console.WriteLine($"Successor id retrieved: {_successor.id}, my id is: {_node.id}");
            await AgnetaHandler.Log(1, $"Successor id retrieved: {_successor.id}, my id is: {_node.id}");

            // Assign new successor
            _successor.predecessor = _node;
            _node.successor = _successor;
            Console.WriteLine($"Updated my successor");
            await AgnetaHandler.Log(1, $"Updated my successor");

            // Build finger table by communicating with successor
            Console.WriteLine($"Building finger table");
            _node = await InitFingerTable(_node);
            Console.WriteLine($"Created new finger table with help from successor");
            await AgnetaHandler.Log(1, $"Created new finger table with help from successor");

            // Get Predecessor from successor
            GetPredecessorService gps = new GetPredecessorService();
            GetPredecessor_Result gps_res = await gps.ClientGet(_successor.ip);
            Console.WriteLine("got predecessor");

            M_Node _predecessor = new M_Node() { id = gps_res.Id, ip = gps_res.Ip };
            _node.predecessor = _predecessor;
            Console.WriteLine($"Predecessor retrieved from successor, id: {_node.predecessor.id}");
            await AgnetaHandler.Log(1, $"Predecessor retrieved from successor, id: {_node.predecessor.id}");

            // Update predecessor
            UpdatePredecessorService ups = new UpdatePredecessorService();
            UpdatePredecessor_Req ups_req = new UpdatePredecessor_Req(){ Id = _node.id, Ip = _node.ip };
            await ups.ClientUpdate(ups_req, _node.successor.ip);
            Console.WriteLine($"Updated successors predecessor");
            await AgnetaHandler.Log(1, $"Updated successors predecessor");

            // Update successor
            UpdateSuccessorService uss = new UpdateSuccessorService();
            UpdateSuccessor_Req uss_req = new UpdateSuccessor_Req(){ Id = _node.id, Ip = _node.ip };
            await uss.ClientUpdate(uss_req, _node.predecessor.ip);
            Console.WriteLine($"Updated predecessors successor");
            await AgnetaHandler.Log(1, $"Updated predecessors successor");

            // Update other
            await UpdateOthers(_node);
            await TestDHT(Globals._NODE);
        }

        Globals._NODE = _node;
    }

    public static async Task<string> FindPeerResponsible(M_Node _node, ulong target)
    {
        // First, handle the simple case - if we're responsible for this target
        if (NodeUtils.inBetween(target, _node.predecessor.id, _node.id))
        {
            Console.WriteLine($"Node {_node.id} is directly responsible for target {target}");
            return _node.ip;
        }
    
        // Second case - if it's between us and our successor
        if (NodeUtils.inBetween(target, _node.id, _node.successor.id))
        {
            Console.WriteLine($"Successor {_node.successor.id} is responsible for target {target}");
            return _node.successor.ip;
        }
    
        // Find the closest preceding finger that could help us
        ulong[] fingerTableKeys = _node.fingerTable.Keys.ToArray();
        M_Node bestCandidate = _node;
        
        for (int i = Globals.FINGER_TABLE_SIZE - 1; i >= 0; i--)
        {
            M_Node finger = _node.fingerTable[fingerTableKeys[i]];
            
            // Skip if the finger points to ourselves
            if (finger.ip == _node.ip)
                continue;
                
            // Skip if the finger points to our successor (we already checked that case)
            if (finger.ip == _node.successor.ip)
                continue;
                
            // If this finger is between us and the target, it's our best candidate
            if (NodeUtils.inBetween(finger.id, _node.id, target))
            {
                bestCandidate = finger;
                break;
            }
        }
    
        // If we couldn't find a better candidate, forward to our successor
        if (bestCandidate.id == _node.id)
        {
            Console.WriteLine($"No better candidate found, forwarding to successor {_node.successor.id}");
            return _node.successor.ip;
        }
    
        // Forward the query to our best candidate
        Console.WriteLine($"Forwarding query for {target} to node {bestCandidate.id}");
        FindPeerResponsibleService fprs = new FindPeerResponsibleService();
        QueryReq req = new QueryReq() { Val = target };
        QueryRes result = await fprs.ClientFind(req, bestCandidate.ip);
        return result.Res;
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
