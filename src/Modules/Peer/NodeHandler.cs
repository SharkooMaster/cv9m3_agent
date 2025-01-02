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

    public static async Task UpdateFingerTable(M_Node _node, M_Node new_node, int finger_index)
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
            }
        }
    }

    public static async Task<M_Node> InitFingerTable(M_Node _node)
    {
        ulong fingerStartBefore = 0;
        for (int i = 1; i < Globals.FINGER_TABLE_SIZE; i++)
        {
            ulong shift = 1UL << i;
            ulong sum = _node.id + shift;
            ulong modulo = (1UL << Globals.FINGER_TABLE_SIZE);
            ulong fingerStart = sum % modulo;

            if(i == 0)
            {
                _node.fingerTable[fingerStart] = _node.successor;
                fingerStartBefore = fingerStart;
            }
            else
            {
                if(NodeUtils.inBetween(fingerStart, _node.id, _node.fingerTable[fingerStartBefore].id))
                {
                    _node.fingerTable[fingerStart] = _node.fingerTable[fingerStartBefore];
                }
                else
                {

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

                    _node.fingerTable[fingerStart] = new M_Node(){ id = gnis_res.Id, ip = gnis_res.Ip };
                }
                fingerStartBefore = fingerStart;
            }
        }
        return _node;
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

            // Build finger table by communicating with successor
            //Console.WriteLine($"Building finger table");
            //await BuildFingerTable(_node);
            //Console.WriteLine($"Created new finger table with help from successor");
            //await AgnetaHandler.Log(1, $"Created new finger table with help from successor");
        }

        Globals._NODE = _node;
    }

    public static async Task<string> FindPeerResponsible(M_Node _node, ulong target)
    {
        bool isFound = false;
        int indexOfResult = -1;

        Console.WriteLine($"FindPeer request recieved, target: {target}");

        // Search finger table for a valid peer
        ulong[] fingerTableKeys = _node.fingerTable.Keys.ToArray();
        for (int i = _node.fingerTable.Count - 1; i >= 0; i--)
        {
            if(NodeUtils.inBetween(target, _node.id, fingerTableKeys[i]))
            {
                isFound = true;
                indexOfResult = i;
            }
        }

        if(isFound && _node.fingerTable[fingerTableKeys[indexOfResult]].ip != _node.ip)
        {
            Console.WriteLine("Found");
            // Ask result found if they are responsible for the key to make sure theres no predecessor thats a better fit, and so on
            FindPeerResponsibleService fprs = new FindPeerResponsibleService();
            QueryReq req = new QueryReq() { Val=target };
            QueryRes result = await fprs.ClientFind(req, _node.fingerTable[fingerTableKeys[indexOfResult]].ip);
            return result.Res;
        }
        else
        {
            Console.WriteLine("Nothing found");
            // either im the peer responsible, or we have an issue
            return _node.ip;
        }
    }
}
