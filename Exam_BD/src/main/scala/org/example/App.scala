package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object App {
  def main(args: Array[String]): Unit = {
    // Initialisation de Spark
    val spark = SparkSession
      .builder
      .appName("GraphExercice")
      .master("local[*]")
      .getOrCreate()

    // Options pour edges.txt
    val optionsEdges = Map("header" -> "false", "inferSchema" -> "true", "delimiter" -> " ")

    // Options pour nodes.csv
    val optionsNodes = Map("header" -> "false", "inferSchema" -> "true", "delimiter" -> ";")

    // Charger edges.txt
    val edgesDataFrame = spark.read.options(optionsEdges).csv("C:/Users/douae/Desktop/edges.txt")

    // Charger nodes.csv
    val nodesDataFrame = spark.read.options(optionsNodes).csv("C:/Users/douae/Desktop/nodes.csv")

    // Convertir DataFrames en RDD
    val edgesRDD: RDD[Edge[String]] = edgesDataFrame.rdd.map(row => Edge(row.getAs[Int](0), row.getAs[Int](1), "relation"))
    val verticesRDD: RDD[(VertexId, (String, String, Int, String))] = nodesDataFrame.rdd.map(row => (row.getAs[Int](0), (row.getAs[String](1), row.getAs[String](2), row.getAs[Int](3), row.getAs[String](4))))

    // Création du graphe
    val graph = Graph(verticesRDD, edgesRDD)

    // Affichage des triplets
    println("Affichage des triplets du graphe:")
    graph.triplets.collect.foreach { triplet =>
      println(s"${triplet.srcAttr} est connecté à ${triplet.dstAttr} avec une relation ${triplet.attr}")
    }

    ////////////////////////////

    // Calcul du PageRank
    val ranks = graph.pageRank(0.0001).vertices

    // Joindre les rangs avec les sommets du graphe pour obtenir les noms
    val ranksWithNames = ranks.join(graph.vertices).map {
      case (id, (rank, (name, _, _, _))) => (id, name, rank)
    }

    // Afficher l'ID, le nom et le PageRank de chaque utilisateur
    println("ID, Nom, PageRank:")
    ranksWithNames.collect.foreach { case (id, name, rank) =>
      println(s"$id, $name, $rank")
    }
//    3) Créer un sous-graphe des arêtes dont les deux sommets ont des zones de résidence différentes et un abonnement Basic

    val basicSubgraph = graph.subgraph(epred = triplet => {
      val srcAttr = triplet.srcAttr
      val dstAttr = triplet.dstAttr
      srcAttr._2 != dstAttr._2 && srcAttr._4 == "Basic" && dstAttr._4 == "Basic"
    })

    // Affichage des arêtes du sous-graphe
    println("Arêtes du sous-graphe (zones résidence différentes, abonnement Basic):")
    basicSubgraph.edges.collect.foreach(println)

//    4) Calculer le trianglecount de chaque utilisateurs et l'afficher ensuite

    // Calculer le triangle count pour chaque sommet
    val triangleCounts = graph.triangleCount().vertices

    // Joindre le triangle count avec les sommets du graphe pour obtenir les noms
    val triangleCountsWithNames = triangleCounts.join(graph.vertices).map {
      case (id, (count, (name, _, _, _))) => (id, name, count)
    }

    // Afficher l'ID, le nom et le triangle count de chaque utilisateur
    println("ID, Nom, TriangleCount:")
    triangleCountsWithNames.collect.foreach { case (id, name, count) =>
      println(s"$id, $name, $count")
    }
//    5) Combien de clusters forment les utilisateurs? donner le code qui le fournit
// Calcul des composantes connexes du graphe
val connectedComponents = graph.connectedComponents().vertices

    // Trouver le nombre de clusters distincts en comptant les labels uniques des composantes
    val numberOfClusters = connectedComponents.map { case (_, component) => component }.distinct().count()

    // Afficher le nombre de clusters
    println(s"Nombre de clusters : $numberOfClusters")

//    6) Utiliser aggregateMessages pour calculer pour chaque utilisateur le nombre personnes avec lesquelles il est en relation et qui sont plus jeunes que lui.
val youngerContactsCounter = graph.aggregateMessages[Int](
  triplet => {
    // Condition pour vérifier si le voisin est plus jeune
    if (triplet.srcAttr._3 > triplet.dstAttr._3) {
      // Si la source est plus âgée, envoyer un message au destinataire (1 pour compter une personne)
      triplet.sendToDst(1)
    }

  },
  // Ajouter le compteur pour chaque sommet
  (a, b) => a + b
)

    // Afficher le résultat
    youngerContactsCounter.collect.foreach { case (vertexId, count) =>
      println(s"Utilisateur $vertexId est en relation avec $count personne(s) plus jeune(s) que lui.")
    }

      // Fermeture de la session Spark
    spark.stop()
  }
//  def main(args: Array[String]): Unit = {
//    // Initialisation de Spark
//    // Initialisation de Spark
//    val spark = SparkSession
//      .builder
//      .appName("App")
//      .master("local[*]") // Utilisez "local[*]" pour l'exécution locale
//      .getOrCreate()
//
//    // Options pour books_edges.csv
//    val optionsEdges = Map("header" -> "false", "inferSchema" -> "true", "delimiter" -> " ")
//
//    // Options pour books_node.csv
//    val optionsNodes = Map("header" -> "false", "inferSchema" -> "true", "delimiter" -> ";")
//
//    // Charger books_edges.csv
//    val edgesDataFrame = spark.read.options(optionsEdges).csv("C:/Users/douae/Desktop/books_edges.csv")
//
//    // Charger books_node.csv
//    val nodesDataFrame = spark.read.options(optionsNodes).csv("C:/Users/douae/Desktop/books_nodes.csv")
//
//    // Convertir DataFrames en RDD
//    val edgesRDD: RDD[Edge[String]] = edgesDataFrame.rdd.map(row => Edge(row.getAs[Int](0).toLong, row.getAs[Int](1).toLong, "cite"))
//    val verticesRDD: RDD[(VertexId, (String, String))] = nodesDataFrame.rdd.map(row => (row.getAs[Int](0).toLong, (row.getAs[String](1), row.getAs[String](2))))
//
//    // Création du graphe
//    val graph: Graph[(String, String), String] = Graph(verticesRDD, edgesRDD)
//
//    // Affichage des arêtes
//    println("Affichage des arêtes:")
//    graph.edges.collect.foreach(println)
//
//    // Affichage des sommets
//    println("Affichage des sommets:")
//    graph.vertices.collect.foreach(println)
//    ////////////////////////////
//
//    // Calculer le PageRank du graphe
//    val ranks = graph.pageRank(0.0001).vertices
//
//    // Joindre les rangs avec les sommets du graphe pour obtenir les titres
//    val ranksWithTitles = ranks.join(graph.vertices).map {
//      case (id, (rank, (title, _))) => (id, title, rank)
//    }
//
//    // Afficher l'ID, le titre et le PageRank de chaque livre
//    ranksWithTitles.collect.foreach { case (id, title, rank) =>
//      println(s"ID: $id, Titre: $title, PageRank: $rank")
//    }
//
//
//    ///////////////////////////
//    // Filtrer les sommets pour ne garder que ceux dont l'idéologie est "neutral"
//    val neutralVerticesRDD = verticesRDD.filter {
//      case (_, (_, ideology)) => ideology == "neutral"
//    }
//
//    // Utiliser subgraph pour créer un sous-graphe avec les sommets filtrés
//    val neutralSubgraph = graph.subgraph(vpred = (id, attr) => attr._2 == "neutral")
//
//    // Affichage des arêtes du sous-graphe
//    println("Affichage des arêtes du sous-graphe des livres neutres:")
//    neutralSubgraph.edges.collect.foreach(println)
//
//    // Affichage des sommets du sous-graphe
//    println("Affichage des sommets du sous-graphe des livres neutres:")
//    neutralSubgraph.vertices.collect.foreach(println)
//
//    ///////////////////////
//    // Calculer le triangle count pour chaque sommet
//    val triangleCounts = graph.triangleCount().vertices
//
//    // Joindre le triangle count avec les sommets du graphe pour obtenir les titres
//    val triangleCountsWithTitles = triangleCounts.join(graph.vertices).map {
//      case (id, (count, (title, _))) => (id, title, count)
//    }
//
//    // Afficher l'ID, le titre et le triangle count de chaque livre
//    println("Triangle count par livre :")
//    triangleCountsWithTitles.collect.foreach { case (id, title, count) =>
//      println(s"ID: $id, Titre: $title, TriangleCount: $count")
//    }
//
//    /////////////////////////////
//    // Calculer les composantes connexes du graphe
//    val connectedComponents = graph.connectedComponents()
//
//    // Trouver le nombre de clusters distincts en comptant les labels uniques
//    val numberOfClusters = connectedComponents.vertices.map(_._2).distinct().count()
//
//    println(s"Nombre de clusters (composantes connexes) : $numberOfClusters")
//
//    // Fermeture de la session Spark
//    spark.stop()
//  }

}
