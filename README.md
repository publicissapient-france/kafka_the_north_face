#Kafka par la face nord


##Notions
Petit rappel des notions de base:

* **publish/subscribe**: pattern d'architecture qui découple production et consommation. Un émetter poste un message dans un broker. On peut avoir la distribution de ce message de 0 à N listeners connectés. C'est le listener qui choisit ce qu'il reçoit.
* **noeud**: un serveur Kafka.
* **cluster**: un ensemble de noeuds Kafka communicant entre eux pour former un service cohérent. Les noeuds se synchronisent au travers de Zookeeper pour se répartit les données et les réplicats.
* **Zookeeper**: peut être vu que une base de registre distribuée hautement disponible et fortement cohérente. Un des outils incontournables dans les architectures master/master.
* **topic**: un topic est un nom logique regroupant des partitions. On stocke dans un topic des messages du même type, facilitant ainsi la consommation par les listeners. Un topic possède des caractéristiques comme le nombre de partitions, le délai de rétention des messages (Kafka fait de la purge automatique), le facteur de réplication...
* **partition**: une partition peut être vue comme une pile de message. Mais contraitement à une queue, ce n'est pas une FIFO. Les messages sont historisés pendant pour la durée de rétention du topic. Ils ne disparaissent pas à la consommation. L'ordre des messages est seulement garanti au sein d'une partition, pas d'un topic. Il n'y a toujours au plus qu'un noeud Kafka leader sur une partition. Il centralise ainsi toutes les écritures. Deux messages envoyés successivement par un même producteur seront stockés dans le même ordre dans la partition.
* **offset**: ID unique d'un message pour un couple topic/partition. C'est un nombre strictement croissant. C'est le point central de la production/consommation de message dans Kafka
* **producteur**: c'est l'émetteur d'un message. Il peut poster un message dans un topic en choississant spécifiquement une partition ou un au hasard.
* **consommateur**: c'est l'écouteur de messages. Il se connecte à un topic/partition et demande à recevoir des messages. Il existe deux API: HighLevel and SimpleConsumer. La première est très simple à mettre en place mais il y a peu de paramètres de gestion de l'offset. La seconde offre un contrôle total au prix de quelques bouts de code à faire. On peut choisir de consommer des messages depuis le premier offset d'une partition, du dernier pour ne recevoir que les suivants, ou d'un offset arbitraire.
* **groupe de consommateur**: c'est un ensemble fonctionnel de consommateurs. Les partitions dans Kafka sont persistentes. Les messages ne disparaissent pas à la consommation. Pourtant, les consommateurs de messages ne veulent pas forcément traiter tous les messages debuit le début de la partition à chaque redémarrage. Un consommateur peut faire un *commit* de son dernier offset lu dans Kafka. Au redémarrage, il suffit d'aller chercher cette valeur est de redémarrer la consommation depuis cet offset. Cet offset est au moins unique pour un couple topic/partition. Mais comme plusieurs consommateurs peuvent lire la même partition du même topic en même temps, et à des vitesses différentes, les offsets sont commités pour le triplet (groupe,topic,partittion).




##Les étapes pour créer un consommateur
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
		optionalBroker.map{leader => new SimpleConsumer(leader.host, leader.port, 10000, 64000, "aClientId")}
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



