#Kafka par la face nord


##Présentation de l'atelier
###Si vous choisissez Scala
Il existe dans Eclipse (avec le Scala Eclipse IDE) comme dans IntelliJ la notion de worksheet. 
L'écran est partagé en deux, un éditeur sur la gauche et une fenêtre d'évaluation sur la droite.
La partie gauche permet d'écrire du code Scala sous forme de scripts. Il sera exécuter à interval régulier. Le résultat de l'exécution du programme sera affiché sur le côté droit de l'écran.

###Si vous choisissez Java
Il n'existe pas encore de REPL en Java < 9, du coup, vous devrez écrire une classe Main pour résoudre les exercices suivants.


## Lancement de l'infrastructure
Un cluster nécessite deux types de noeuds: Zookeeper et Kafka.

Pour cet atelier, nous vous conseillons d'ouvrire 6 sessions de terminal, 1 pour Zookeeper, 2 pour Kafka, 3 pour passer des commandes.

NB: TOUTES les commandes assument que vous êtes déjà dans le répertoire des binaires Kafka.

### Lancement de Zookeeper

Zookeeper sert à la coordination de Kafka. Il faut donc les démarrer en premier. Pour les besoins de l'atelier, un seul noeud suffit.

    //Démarrez le noeud avec la configuration par défaut
    ./bin/zookeeper-server-start.sh config/zookeeper.properties
    
Dans une autre session de terminal, vous pouvez tester le noeud avec
    
    telnet 127.0.0.1 2181
    
    //Résultat attendu
    Trying 127.0.0.1...
    Connected to localhost.
    Escape character is '^]'.
    
    
### Lancement de Kafka
    
Nous allons créer deux brokers pour les besoins de l'exercice. Ce sont ces noeuds qui vont gérer le stockage des messages et leur distribution.
    
    //Changez la configuration par défaut avec 4 partitions
    vi config/server.properties
    ...
        num.partitions=4
    ...
    
    //Copiez le fichier dans deux fichiers séparés, un par broker
    cp config/server.properties config/server-1.properties
    cp config/server.properties config/server-2.properties
    
    //Éditez le fichier config/server-1.properties pour changer son id, port et répertoire de travail
    vi config/server-1.properties
    ...
    broker.id=1
    ...
    port=9092
    ...
    log.dirs=/tmp/kafka-logs-1
    ...
    zookeeper.connect=localhost:2181
    ...
    
    //Éditez le fichier config/server-2.properties pour changer son id, port et répertoire de travail
    vi config/server-2.properties
    ...
    broker.id=2
    ...
    port=9093
    ...
    log.dirs=/tmp/kafka-logs-2
    ...
    zookeeper.connect=localhost:2181
    ...
    
Ensuite, dans deux sessions séparées, lancez les deux brokers
    
    //Broker 1
    //Lancez le noeud
    ./bin/kafka-server-start.sh config/server-1.properties
    
    
    ...
    
    //Broker 2
    //Lancez le noeud
    ./bin/kafka-server-start.sh config/server-2.properties
    
Vérification
    
    //Connectez vous à Zookeper
    ./bin/zookeeper-shell.sh 127.0.0.1:2181
    
    //Vérifiez les ids des brokers
    ls /brokers/ids
    
    //Vérifiez le port du broker1
    get /brokers/ids/1
    
    //Vérifiez le port du broker2
    get /brokers/ids/2
    
    
### Création d'un topic
    
Nous allons créer un topic pour cet atelier. Il sera répliqué une seule fois avec 4 partitions

    ./bin/kafka-topics.sh --create --topic xebicon --partition 4 --replication-factor 1 --zookeeper 127.0.0.1:2181
    
Vous remarquerez que nous de discutons par directement avec Kafka mais seulement avec Zookeeper. 
Kafka est par nature distribué, chaque broker surveille Zookeeper. L'ensemble se coordonne ensuite pour avec un état cohérent.
Ainsi après la création du topic, vous avez du voir des logs passer dans les brokers Kafka.

Nous allons maintenant vérifier notre topic:

    ./bin/kafka-topics.sh --describe  --topic xebicon --zookeeper 127.0.0.1:2181
    Topic:xebicon	PartitionCount:4	ReplicationFactor:1	Configs:
    	Topic: xebicon	Partition: 0	Leader: 	Replicas: 2	Isr: 2
    	Topic: xebicon	Partition: 1	Leader: 	Replicas: 1	Isr: 1
    	Topic: xebicon	Partition: 2	Leader: 	Replicas: 2	Isr: 2
    	Topic: xebicon	Partition: 3	Leader: 	Replicas: 1	Isr: 1
    	
Dans mon cas, les partitions 0 et 2 sont gérées par le broker 2, 1 et 3 par le broker 1. 
Notez qu'il n'y a pas encore de leader.


### Lancement d'un consommateur en CLI

La distribution Kafka vient avec un consommateur basique en ligne de commande. Nous allons écoutez les messages de notre topic.
 
    ./bin/kafka-console-consumer.sh --topic xebicon --zookeeper 127.0.0.1:2181
    
Rien ne se passe pour l'instant car il n'y a pas de production de données. Par contre, vous aurez remarqué que des logs sont apparus dans les brokers.
Maintenant qu'il y a du trafic sur Kafka, chaque partition a trouvé son leader.

    ./bin/kafka-topics.sh --describe  --topic xebicon --zookeeper 127.0.0.1:2181
    Topic:xebicon	PartitionCount:4	ReplicationFactor:1	Configs:
        Topic: xebicon	Partition: 0	Leader: 2	Replicas: 2	Isr: 2
        Topic: xebicon	Partition: 1	Leader: 1	Replicas: 1	Isr: 1
        Topic: xebicon	Partition: 2	Leader: 2	Replicas: 2	Isr: 2
        Topic: xebicon	Partition: 3	Leader: 1	Replicas: 1	Isr: 1

### Lancement d'un producteur en CLI
    
De la même façon qu'il y a un consommateur basique, il y a aussi un producteur basique en chaînes de caractères. Une fois la commande lancée, vous pouvez écrire des messages qui seront envoyés après l'appel à 'Enter'.
    
    ./bin/kafka-console-producer.sh --topic xebicon --broker-list 127.0.0.1:9092,127.0.0.1:9093
    
Vous remarquerez cette fois que nous avons donné en paramètre la liste des brokers et pas de zookeeper.

Lancez au moins 4 messages à la suite. Ils apparaissent maintenant dans la fenêtre du consommateur. Allons maintenant voir le stockage.

    ls -l /tmp/kafka-logs-1 /tmp/kafka-logs-2
    /tmp/kafka-logs-1:
    total 16
    .
    ..
    recovery-point-offset-checkpoint
    replication-offset-checkpoint
    xebicon-1
    xebicon-3
    
    /tmp/kafka-logs-2:
    total 16
    .
    ..
    recovery-point-offset-checkpoint
    replication-offset-checkpoint
    xebicon-0
    xebicon-2
   
Dans chaque répertoire de travail, un répertoire par partition a été créé. 
       
    ls -l /tmp/kafka-logs-*/xebicon-*/*
    /tmp/kafka-logs-1/xebicon-1/00000000000000000000.index
    /tmp/kafka-logs-1/xebicon-1/00000000000000000000.log
    /tmp/kafka-logs-1/xebicon-3/00000000000000000000.index
    /tmp/kafka-logs-1/xebicon-3/00000000000000000000.log
    /tmp/kafka-logs-2/xebicon-0/00000000000000000000.index
    /tmp/kafka-logs-2/xebicon-0/00000000000000000000.log
    /tmp/kafka-logs-2/xebicon-2/00000000000000000000.index
    /tmp/kafka-logs-2/xebicon-2/00000000000000000000.log
    
Dans chaque partition, un fichier a été créé pour indexer les données et les stocker.

    cat /tmp/kafka-logs-*/xebicon-*/*.log
    p_N����Bonjour Xebiconp_N����Bonjour Xebiconp_N����Bonjour Xebicon�������phae�[^<����ahize%
    
Enfin, à l'intérieur des fichiers se trouvent les messages.


Maintenant que nous avons validé l'installation, nous allons codé :-)

##Codons un producteur de données
Vous avez déjà créé un producteur et un consommateur de données depuis la console avec les outils de Kafka.
Nous allons maintenant créer un producteur de données avec du code et l'API 0.8.2.X de Kafka.	
Kafka est par nature distribué et dynamique. Ainsi, le seul élément stable dans un cluster Kafka est l'adresse des noeuds Zookeeper. 
Tout y est stocké, la liste des noeuds, des topics, des partitions, des noeuds leaders de partitions.
La liste des étapes pour parvenir à écrire dans Kafka est:

* création d'un client Zookeeper
* récupération des adresses des noeuds Kafka (ou brokers)
* instanciation d'un KafkaProducer
* écriture dans le topic

### Création d'un client Zookeeper

En supposant que votre Zookeeper est bien sur 127.0.0.1:2181, créer un client Zookeeper pour Kafka se fait comme suit:

	import kafka.utils.ZKStringSerializer
	import org.I0Itec.zkclient.ZkClient

	val zkClient = new ZkClient("127.0.0.1:2181", 10000, 5000, ZKStringSerializer)


	...


	zkClient.close()
	
	
Résultat attendu:

	import kafka.utils.ZKStringSerializer
    import org.I0Itec.zkclient.ZkClient
    
    
    zkClient: org.I0Itec.zkclient.ZkClient = org.I0Itec.zkclient.ZkClient@427cfaef
    
    
    res0: Unit = ()


Vous pouvez déjà inclure l'appel à **client.close()** pour libérer les ressources à la fin de votre programme.

NB: dans un environnement de production, il est nécessaire de préciser l'adresse de l'ensemble des noeuds Zookeeper. 
Zookeeper préfère ne pas donner de valeur plutôt qu'une mauvaise. Ainsi un client Zookeeper de connaître l'ensemble des noeuds pour toujours avoir la bonne version de la donnée.
Dans un environnement distribué, chaque noeud pour accepter des modifications de données, même concurrente. Zookeeper est bâti pour toujours répondre quand une majorité de noeuds de son cluster est d'accord sur la réponse.

NB: cette portion n'est pas vraiment obligatoire. Dans un environnement où les noeuds Kafka sont stables avec IP,PORT connus, il n'est pas nécessaire d'aller les chercher dans Zookeeper.
Cette partie de "service discovery" est néanmoins un classique dans les systèmes distribués et reste nécessaire pour le SimpleConsumer. Vous n'aurez donc pas perdu votre temps :)

### Récupération des adresses des brokers

Maintenant que vous avez un client Zookeeper, vous devez y trouver les brokers Kafka. 
Kafka fournit l'API pour le faire grâce à la classe *kafka.utils.ZkUtils*. Vous aurez alors la liste de tous les brokers du système.
Il vous faudra ensuite concatener avec ',' la liste des attributs *connectionString* de chaque broker.

Petite aide en Scala:
	val connectionString = brokers.map(_.connectionString).mkString(",")

Solution:

	import kafka.utils.ZkUtils
	
	val brokers = ZkUtils.getAllBrokersInCluster(zkClient)
    val connectionString = brokers.map(_.connectionString).mkString(",")	
    
Résultat attendu:

    import kafka.utils.ZKStringSerializer
    import org.I0Itec.zkclient.ZkClient
    import kafka.utils.ZkUtils
    
    
    zkClient: org.I0Itec.zkclient.ZkClient = org.I0Itec.zkclient.ZkClient@f41de1b
    brokers: Seq[kafka.cluster.Broker] = ArrayBuffer(id:0,host:192.168.59.3,port:9092)
    connectionString: String = 192.168.59.3:9092 // <-- LA LISTE AVEC VOTRE BROKER
    
    
    res0: Unit = ()

	

### Instanciation d'un KafkaProducer

La classe à utiiser dans Kafka est *org.apache.kafka.clients.producer.KafkaProducer<K,V>*
Il n'y a pas grand chose à deviner à cette étape. On y retrouve quelques paramètres d'un producteur Kafka.

	import org.apache.kafka.clients.producer.KafkaProducer

	val props = Map(
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "partitioner.class" -> "kafka.producer.DefaultPartitioner",
      "max.request.size" -> "10000",
      "producer.type" -> "sync",
      "bootstrap.servers" -> connectionString,
      "acks" -> "all",
      "retries" -> "3",
      "retry.backoff.ms" -> "500")

    import scala.collection.convert.wrapAsJava._
    val producer = new KafkaProducer[Any, Any](props)

Il s'agit ici d'un producteur de données String, String. Chaque message peut avoir une clé et possède forcément une valeur. 
* *org.apache.kafka.common.serialization.StringSerializer* se contente de passer tout le contenu du message de type String en clair sur le disque. 
* *kafka.producer.DefaultPartitioner* permet de choisir la stratégie de routage des messages d'un topic vers une partition. Ici c'est du RoundRobin.
* *max.request.size* est un paramètre important. Il s'agit de la taille maximum autorisée. Au dessus, le producteur lancer une exception. Nous verrons son importance côté consommateur plus tard dans l'exercice.
* *producer.type* crée ici un producteur synchrone. Cela permet d'avoir les Ack du cluster mais par contre empêche l'utilisation en mode "batch".

L'import de *wrapAsJava._* peut sembler magique. Il permet seulement de convertir la Map Scala en Map Java.

Résultat attendu:
	
	import java.util.concurrent.TimeUnit
    
    import kafka.utils.ZKStringSerializer
    import org.I0Itec.zkclient.ZkClient
    import kafka.utils.ZkUtils
    import org.apache.kafka.clients.producer.KafkaProducer
    import org.apache.kafka.clients.producer.ProducerRecord
    
    zkClient: org.I0Itec.zkclient.ZkClient = org.I0Itec.zkclient.ZkClient@1ad1a482
    brokers: Seq[kafka.cluster.Broker] = ArrayBuffer(id:0,host:192.168.59.3,port:9092)
    connectionString: String = 192.168.59.3:9092
    
    props: scala.collection.immutable.Map[String,String] = Map(producer.type -> sync, retries -> 3, partitioner.class -> kafka.producer.DefaultPartitioner, max.request.size -> 10000, bootstrap.servers -> 192.168.59.3:9092, value.serializer -> org.apache.kafka.common.serialization.StringSerializer, acks -> all, retry.backoff.ms -> 500, key.serializer -> org.apache.kafka.common.serialization.StringSerializer)
    
    import scala.collection.convert.`package`.wrapAsJava._
    producer: org.apache.kafka.clients.producer.KafkaProducer[Any,Any] = org.apache.kafka.clients.producer.KafkaProducer@5f1ff152
    
	res0: Unit = ()
    	
    	

### Écriture dans le topic

L'envoie d'un message dans Kafka avec un KafkaProducer se fait à l'aide d'un objet de type *org.apache.kafka.clients.producer.ProducerRecord<K, V>*.

Explorez l'API de KafkaProducer et ProducerRecord et envoyez un message à l'aide d'un ProducerRecord[Any,Any] sur le topic **xebicon**.

Démarrer une session de console consumer sur le topic *xebicon*, vous devriez voir votre message passé.

	/bin/kafka-console-consumer.sh --zookeeper 127.0.0.1:2181 --topic xebicon

SOLUTION:
	import java.util.concurrent.TimeUnit
	import org.apache.kafka.clients.producer.ProducerRecord

	val recordMetadata = producer.send(new ProducerRecord[Any, Any]("xebicon", "hello")).get(5, TimeUnit.SECONDS)
	
	s"message persisted to: ${recordMetadata.topic()}, ${recordMetadata.partition()}, ${recordMetadata.offset()}"
	
Résultat attendu:
	
	import java.util.concurrent.TimeUnit
    
    import kafka.utils.ZKStringSerializer
    import org.I0Itec.zkclient.ZkClient
    import kafka.utils.ZkUtils
    import org.apache.kafka.clients.producer.KafkaProducer
    import org.apache.kafka.clients.producer.ProducerRecord
    
    zkClient: org.I0Itec.zkclient.ZkClient = org.I0Itec.zkclient.ZkClient@1ad1a482
    brokers: Seq[kafka.cluster.Broker] = ArrayBuffer(id:0,host:192.168.59.3,port:9092)
    connectionString: String = 192.168.59.3:9092
    
    props: scala.collection.immutable.Map[String,String] = Map(producer.type -> sync, retries -> 3, partitioner.class -> kafka.producer.DefaultPartitioner, max.request.size -> 10000, bootstrap.servers -> 192.168.59.3:9092, value.serializer -> org.apache.kafka.common.serialization.StringSerializer, acks -> all, retry.backoff.ms -> 500, key.serializer -> org.apache.kafka.common.serialization.StringSerializer)
    
    import scala.collection.convert.`package`.wrapAsJava._
    producer: org.apache.kafka.clients.producer.KafkaProducer[Any,Any] = org.apache.kafka.clients.producer.KafkaProducer@5f1ff152
    
    recordMetadata: org.apache.kafka.clients.producer.RecordMetadata = org.apache.kafka.clients.producer.RecordMetadata@69d0a23f
    
    res0: String = message persisted to: xebicon, 0, 7
    
    res1: Unit = ()


### Écriture de façon continue dans le topic


Pour rendre l'exercice plus interactif, nous allons créer un stream de données pour notre topic *xebicon* représentant la charge moyenne du système à interval régulier.

NB: vérifier dans vos préférences projet et IntelliJ/Eclipse que le projet et le compilateur Scala sont en JDK 1.8.

Solution:

    import java.time.Instant
    import java.lang.management.ManagementFactory
    
    Stream.continually(Instant.now()).foreach { instant =>
      val averageSystemLoad = ManagementFactory.getOperatingSystemMXBean.getSystemLoadAverage
      producer.send(new ProducerRecord[Any, Any]("xebicon", s"$instant: avg_load: $averageSystemLoad")).get(5, TimeUnit.SECONDS)
    
      Thread.sleep(1000)
    }


Résultat attendu dans la console:

    2015-10-20T06:11:17.412Z: avg_load: 2.81005859375
    2015-10-20T06:11:18.934Z: avg_load: 2.81005859375
    2015-10-20T06:11:19.939Z: avg_load: 2.81005859375
    2015-10-20T06:11:20.946Z: avg_load: 2.6650390625
    2015-10-20T06:11:21.951Z: avg_load: 2.6650390625


##Codons un consommateur haut niveau
### Récupération de la configuration du cluster pour un topic
Toute la configuration du cluster est mise à jour par Kafka dans Zookeeper. Pour pouvoir consommer des messages, il faut récupérer dans les metadata du topic le nombre de partitions configurés. On rappelle qu'un topic n'est qu'un ensemble de partition, chaque partition étant une "file" de messages persistante.

Il faut:

* créer un client Zookeeper
* Chercher dans kafka.admin.AdminUtils la bonne méthode


	// Réponse
	val topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkClient)

 	
 ### Se connecter à une partition
 Il existe à un instant au plus 1 noeud Kafka leader pour une partition d'un topic donné. 
 Pour faire simple, nous allons nous connecter à toutes les partitions du topic en une fois. Il faudra donc faire une boucle sur la liste des partitions que vous avez récupérer précédemment.

Il faut: 

* fouiller dans la réponse précédente pour trouver chaque leader de chaque partition. 
* se connecter au broker en instantiant un kafka.consumer.SimpleConsumer par partition.

	// Réponse
	val partitionsBroker: Map[Int, Option[Broker]] = topicMetadata.partitionsMetadata.groupBy(_.partitionId).toMap.mapValues(_.head.leader)

	val partitionsConsumer: Map[Int, Option[SimpleConsumer]] = partitionsBroker.mapValues{optionalBroker => 
		optionalBroker.map{leader => new SimpleConsumer(leader.host, leader.port, 10000, 64000, "aCLientId")}
	}

### Trouver l'offset de démarrage de consommation
Une partition est un journal en ajout seulement. Chaque message possède un numéro unique au sein d'une même partition. Cet identifiant, issu d'un compteur monotonique (strictement croissant), est nommé offset. 
Pour chaque requête de données à Kafka, on lui précise le nombre de messages que l'on veut recevoir, et depuis quelle position, offset.

Il faut: 

	* trouver sur SimpleConsumer une méthode nous permettant de trouver l'identifiant du premier offset connu de chaque partition.

	//Réponse
	consumer.earliestOrLatestOffset(topicAndPartition, OffsetRequest.EarliestTime, Request.OrdinaryConsumerId)	


NB: on pourrait aussi lancer le consommateur depuis la fin courante de la file. Ainsi, le consommateur ne recevrait de messages que lorsqu'un nouveau serait posté.

### Faire une requête de données
Maintenant que nous avons la connexion au leader et l'offset à demander, il n'y a plus qu'à récupérer les infos. Dans Kafka, on ne demande pas N messages. On demande une taille à récupérer. Dans la réponse, nous aurons ensuite un itérateur permettant de parcourir chaque message reçu. Il est donc **important** de connaître la taille des messages que l'on manipule. Cela semble bizarre au début mais cela se révèle être un atout majeur en terme de performance. En effet, toutes les I/O se mesurent en Bytes, network, buffer, disque... en ne manipulant que des tailles en bytes, il est ainsi d'être le plus précis possible pour le tuning de performance.

Il faut

	* créer une FetchRequest grâce au FetchRequestBuilder. 
	* l'exécuter avec le SimpleConsumer
	* itérer sur l'Iterator de MessageSet 

	//Réponse
	val request = new FetchRequestBuilder()
        .clientId(groupId)
        .addFetch(topic, partitionId, nextOffsetToFetch, maxMessageSize * count)
        .maxWait(fetchTimeout)
        .build()

    val fetchReply = consumer.fetch(request)
    

NB: il est possible que Kafka vous envoie des messages un peu avant l'offset qui est demandé (pour des raisons d'optimisation). Si le côté transactionnel est important pour vous, pensez à filtrer sur les offsets des messages reçus.   


### Commit

Vous l'aurez ainsi remarqué, c'est le consommateur qui a la responsabilité de maintenir l'offset de lecture. Le broker Kafka ne sait pas à priori qui a déjà consommé quoi.
Il existe deux façons proposées par Kafka pour maintenir cette information, mais vous pouvez utiliser la votre. Il suffit juste de maintenir quelque part ce fameux offset de consommation.
Initialement, Kafka stockait les offsets dans Zookeeper. Cette solution fortement cohérente en système distribué s'est avéré trop peu performante. 
La seconde solution proposée par Kafka est de stocké lui même l'offset dans un topic maintenu par le cluster. Il existe un noeud particulier dans le cluster qui joue le rôle du coordinateur à qui on peut demander les offsets et de "commiter" un offset pour un groupe de consommateurs, topic et partition.

Pour trouver ce coordinateur

Il faut:

	* boucler sur la liste des brokers et s'arrêter au premier qui fonctionne (ou recommencer jusqu'à ce que cela fonctionne)
	* créer un blockingChannel sur un broker
		val channel = new BlockingChannel(host, port, bufferSize, bufferSize, socketTimeout)
        channel.connect()
	* Faire une requête ConsumerMetadataRequest 
		channel.send(new ConsumerMetadataRequest(groupId))
        val reply = ConsumerMetadataResponse.readFrom(channel.receive().buffer)
    * S'il existe un coordinateur, il faut s'y connecter
    * Faire un commit 
	    val request = OffsetCommitRequest(
	      groupId,
	      Map(topicAndPartition -> OffsetAndMetadata(offset)),
	      versionId = 1
	    )

	    println(s"Committing offset <$offset> to partition <$partition>:<$groupId>")
	    val reply = Try {
	      channel.send(request)
	      OffsetCommitResponse.readFrom(channel.receive().buffer)
	    }

	    reply.map(_.commitStatus(topicAndPartition)).filter(_ == NoError)

Pour lire cette valeur et ainsi recommencer à lire depuis le dernier offset connu, il faut :

	* sur le channel du coordinateur, faire une requête OffsetFetchRequest
		val request = OffsetFetchRequest(groupId, List(topicAndPartition))
		channel.send(request)
        OffsetFetchResponse.readFrom(channel.receive().buffer)

Vous pouvez ainsi récupérer le dernier offset connu, à la prochaine requête, vous pourez utiliser cette valuer.



## Et ce n'est pas fini!

Il manque encore plein de choses dans cette implem. Le cluster est dynamique, le coordinateur peut changer de noeud, les partitions peuvent être réassignées sur un autre noeud. Il faut

* Écouter les événements depuis ZK pour suivre les assignements des partitions
* Réessayer plusieurs fois certaines action quand le cluster n'est pas stable (en phase de transition)
* Il se peut que'offset commité n'existe plus dans Kafka, il faut ainsi s'assurer qu'il existe supérieur au premier offset connu...



