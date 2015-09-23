import kafka.admin.AdminUtils
import kafka.admin.AdminUtils.fetchTopicMetadataFromZk
import org.I0Itec.zkclient.ZkClient

val zkClient = new ZkClient("127.0.0.1:2181", 2000)
val topicMetadata = AdminUtils.fetchTopicMetadataFromZk("data", zkClient)
topicMetadata.partitionsMetadata.map(_.leader)
zkClient.close()