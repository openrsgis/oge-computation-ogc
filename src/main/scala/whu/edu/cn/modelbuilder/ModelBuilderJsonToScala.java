package whu.edu.cn.modelbuilder;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;

import java.util.ArrayList;

public class  ModelBuilderJsonToScala {

    private DAGObject dagObject;

    /**
     * init the writer
     */
    public void initWriter() {
        dagObject = new DAGObject();
    }


    /**
     * main function to realize the Dag json to scala file
     * @param DagJson Dag JSON from client
     */

    public DAGObject JsonToScala(JSONObject DagJson) {
        initWriter();
        JSONArray nodeDataArray = DagJson.getJSONArray("nodeDataArray");
        JSONArray linkDataArray = DagJson.getJSONArray("linkDataArray");
        ArrayList <String> existNodes = new ArrayList<>();
        ArrayList <String> toNodeList = getToNodeList(linkDataArray);
        findAndWriterNode(nodeDataArray, linkDataArray, existNodes, toNodeList);
        return dagObject;
    }


    /**
     * find the node that is going to be written in scala file and write them
     * @param nodeDataArray all computer nodes
     * @param linkDataArray a array that contains all links which describe the relationship with each nodes
     * @param existNodes a array that contains the ids of nodes which already have been written
     * @param toNodeList a array that contains all nodes which is the position "to" in links
     */
    public void findAndWriterNode(JSONArray nodeDataArray, JSONArray linkDataArray, ArrayList<String> existNodes, ArrayList <String> toNodeList) {
        JSONArray searchNodeDataArray = nodeDataArray;
        int uid = 0;
        while (searchNodeDataArray.size()!=0){
            for( int index = 0; index < searchNodeDataArray.size(); index++){
                JSONObject nodeData = searchNodeDataArray.getJSONObject(index);
                // 如果该节点没有写
                String nodeId = nodeData.getString("id");
                if(!existNodes.contains(nodeId)){
                    //检查该节点是否可写
                    // to 里没有该节点的id
                    if(!toNodeList.contains(nodeId)){
                        nodeDataArray = writeScalaFile(nodeData, nodeDataArray, index, uid);
                        existNodes.add(nodeId);
                        searchNodeDataArray = (JSONArray) JSONPath.eval(searchNodeDataArray,"$[?(@.id!=\""+nodeId+"\")]");
                    }else{   // to 里有，但是其对应的from已经在existNodes里了
                        JSONObject linksObject =  fromNodesOfTo(nodeId, linkDataArray, existNodes);
                        if(linksObject.getBoolean("ifReady")){
                            nodeDataArray = writeScalaFile(linksObject.getJSONArray("links"), nodeDataArray, index, uid);
                            existNodes.add(nodeId);
                            searchNodeDataArray = (JSONArray) JSONPath.eval(searchNodeDataArray,"$[?(@.id!=\""+nodeId+"\")]");
                        }
                    }
                }
                uid++;
            }
        }

    }


    /**
     * get all the all nodes which are at the position "to" in a link
     * @param linkDataArray a array that contains all links which describe the relationship with each nodes
     * @return toNodeList  a array that contains all nodes which are at the position "to" in a link
     */
    public ArrayList<String> getToNodeList(JSONArray linkDataArray){
        ArrayList<String> toNodeList = new ArrayList<>();
        for(int index=0; index<linkDataArray.size(); index++){
            toNodeList.add(linkDataArray.getJSONObject(index).getString("to"));
        }
        return toNodeList;
    }


    /**
     * get all "from" position nodes with a certain node at "to" position and judge if this "to" node is ready to be written
     * @param nodeId the node id of "to" node
     * @param linkDataArray  a array that contains all links which describe the relationship with each nodes
     * @param existNodes a array that contains the ids of nodes which already have been written
     * @return resultObject
     */
    public JSONObject fromNodesOfTo(String nodeId, JSONArray linkDataArray, ArrayList<String> existNodes){
        boolean ifReady = true;
        JSONArray usedLinkArray = new JSONArray();
        for(int index = 0; index < linkDataArray.size(); index++){
           JSONObject linkData =  linkDataArray.getJSONObject(index);
           if(linkData.getString("to").equals(nodeId)){
               String fromNodeId = linkData.getString("from");
               if(!existNodes.contains(fromNodeId)){
                   ifReady = false;
                   break;
               }else{
                   usedLinkArray.add(linkData);
               }
           }
        }
        JSONObject resultObject = new JSONObject();
        resultObject.put("ifReady", ifReady);
        resultObject.put("links", usedLinkArray);
        return resultObject;
    }


    /**
     *  write a node, which is a single node, into the  scala file
     * @param nodeData target node
     * @param nodeDataArray all nodes
     * @param index the index of this node in the search array
     * @param uid the id of this node in all nodes
     * @return the updated node array
     */
    public JSONArray writeScalaFile(JSONObject nodeData, JSONArray nodeDataArray, int index, int uid) {
        dagObject.saveOperatorHead("RDD_"+uid,  nodeData.getString("name"));
        JSONArray inputsArray = nodeData.getJSONArray("inputs");
        for (int arrayIndex=0; arrayIndex<inputsArray.size(); arrayIndex++){
            JSONObject inputObject = inputsArray.getJSONObject(arrayIndex);
            dagObject.addOperator(inputObject);
        }
        dagObject.clearTemp();
        return updateNodeDataArray(nodeData, nodeDataArray, index, uid);
    }


    /**
     * write a node, which is not a single node, into the  scala file
     * @param usedLinkDataArray a array contains links which are related with this target node
     * @param nodeDataArray all nodes
     * @param index the index of this node in the search array
     * @param uid the id of this node in all nodes
     * @return the updated node array
     */
    public JSONArray writeScalaFile(JSONArray usedLinkDataArray, JSONArray nodeDataArray, int index, int uid ){
        String nodeId = usedLinkDataArray.getJSONObject(0).getString("to");
        String jsonPathFindNode = "$[?(@.id=\""+nodeId+"\")]";
        JSONObject nodeData =  ((JSONArray)JSONPath.eval(nodeDataArray, jsonPathFindNode)).getJSONObject(0);
        dagObject.saveOperatorHead("RDD_"+uid,  nodeData.getString("name"));
        JSONArray inputsArray = nodeData.getJSONArray("inputs");
        //遍历所有的输入列表，如果输入id在linkDataArray里有，那么就从那取，如果不在则
        for(int i=0; i<inputsArray.size(); i++){
            JSONObject inputObject = inputsArray.getJSONObject(i);
            String id = inputObject.getString("id");
            String findInputIdInLinks = "$[?(@.targetVariable=\""+id+"\")]";
            JSONArray linkDataArray = (JSONArray) JSONPath.eval(usedLinkDataArray, findInputIdInLinks);
            //如果存在
            if(linkDataArray.size()!=0){
                JSONObject linkData = linkDataArray.getJSONObject(0);
                String targetInput = linkData.getString("targetVariable");
                String fromNodeId = linkData.getString("from");
                String jsonPathFindFromNode = "$[?(@.id=\""+fromNodeId+"\")]";
                JSONObject fromNodeData = ((JSONArray) JSONPath.eval(nodeDataArray, jsonPathFindFromNode)).getJSONObject(0);
                dagObject.addOperator(targetInput, fromNodeData.getJSONArray("outputs").getJSONObject(0).getString("proxy"));
            }else{
                dagObject.addOperator(inputObject);
            }
        }
        dagObject.clearTemp();
        return updateNodeDataArray(nodeData, nodeDataArray, index, uid);
    }



    /**
     * update the node data array, that is add the "poxy" in "outputs" property of the written node
     * @param nodeData the written node
     * @param nodeDataArray all nodes
     * @param index the index of this node in the search array
     * @param uid the id of this node in all nodes
     * @return newNodeDataArray the node data array which certain node has been updated
     */
    public JSONArray updateNodeDataArray(JSONObject nodeData, JSONArray nodeDataArray, int index, int uid){
        JSONArray newOutputArray = new JSONArray();
        // 如果输出的部分长度不是0
        if( nodeData.getJSONArray("outputs").size()!=0){
            JSONObject outputObject = nodeData.getJSONArray("outputs").getJSONObject(0);
            outputObject.put("proxy", "RDD_" + uid);
            newOutputArray.add(outputObject);
            nodeData.put("outputs", newOutputArray);
            String jsonPath = "$[?(@.id!=\""+nodeData.getString("id")+"\")]";
            JSONArray newNodeDataArray = (JSONArray) JSONPath.eval(nodeDataArray, jsonPath);
            newNodeDataArray.add(index, nodeData);
            return newNodeDataArray;
        }else{
            // 如果长度为0,直接返回
            return nodeDataArray;
        }
    }

}
