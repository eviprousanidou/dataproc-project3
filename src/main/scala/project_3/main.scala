package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {

	//Initialize remaining_vertices
	var remaining_vertices = g_in.numVertices

	while (remaining_vertices >= 1) {
        	// To Implement
		val d = g_in.aggregateMessages[(Int, Int)](
		t => {
		if (t.srcAttr != -1 && t.srcAttr != 1) {
			if (t.dstAttr != -1 && t.dstAttr != 1)
          		{
			t.sendToDst((1, 0))
            		t.sendToSrc((1, 0))
           		}
          		else {
            		t.sendToSrc((0, 0))
            		t.sendToDst((-1, t.dstAttr))
          		}
        	}
        	else if (t.dstAttr != -1 && t.dstAttr != 1){
          		t.sendToDst((0, 0))
          		t.sendToSrc((-1, t.srcAttr))
        	}
        	else {
          		t.sendToSrc((-1, t.srcAttr))
          		t.sendToDst((-1, t.dstAttr))
        	}
		}, ((a,b) => {
      			if (a._1 >= 0)	(a._1 + b._1, 0)
     			else	a
    		})
		)
    	}

	val random = new scala.util.Random()
	
	val random_d = d.map(x => {
		if (x._2._1 == 0)	(x._1, (x._2._1, 1, x._2._2))
    		if (x._2._1 < 0) 	(x._1, (x._2._1, 0, x._2._2))

      		val n = random.nextFloat
      		if (n < 1.0/(2*x._2._1))	(x._1, (x._2._1, 1, x._2._2))
      		else	(x._1, (x._2._1, 0, x._2._2))
	})

	val g_in_new = Graph(random_d, g_in.edges)

	val new_d = g_in_new.aggregateMessages[Int]( t => {
        	if (t.srcAttr._3 != 0 && t.dstAttr._3 != 0) {
          		if (t.srcAttr._3 == 1)	t.sendToSrc(0)
			else	t.sendToSrc(-1)

			if (t.dstAttr._3 == 1)	t.sendToDst(0)
			else t.sendToDst(-1)
        	}
        	else if (t.srcAttr._3 != 0){
          		if (t.srcAttr._3 == 1)	t.sendToSrc(0)
			else	t.sendToSrc(-1)
          		if (t.dstAttr._2 == 1)	 t.sendToDst(0)
          		else	t.sendToDst(1)
        	}
        	else if (t.dstAttr._3 != 0){
          		if (t.dstAttr._3 == 1)	 t.sendToDst(0)
			else	t.sendToDst(-1)

          		if (t.srcAttr._2 == 1)	t.sendToSrc(0)
          		else	t.sendToSrc(1)
        	}
       		else{
          		if (t.srcAttr._2 == 1 && t.dstAttr._2 == 1) {
            			if (t.srcAttr._1 >= t.dstAttr._1){
              				triplet.sendToSrc(0)
              				triplet.sendToDst(1)
            			}
            			else{
              				triplet.sendToSrc(1)
              				triplet.sendToDst(0)
            			}
          		}
          		else if (t.srcAttr._2 == 1){
            			triplet.sendToSrc(0)
            			triplet.sendToDst(1)
          		}
          		else if (t.dstAttr._2 == 1){
            			t.sendToSrc(1)
            			t.sendToDst(0)
         		}
          		else{
            			t.sendToSrc(1)
            			t.sendToDst(1)
          		}
		}
	}, ((a, b) => {
        	if (a < 0)	-1
		else	a+b
	}) ).map(x => {
        	if (x._2 == 0)	(x._1, 1)
        	else if (x._2 == -1)	x
		else	(x._1, 0)
        })


	val g_in_newer = Graph(new_d, g_in.edges)

	val no_neighbors = g_in_newer.aggregateMessages[Int](	(t => {
        	if (t.srcAttr == 1){
          		t.sendToDst(-1)
          		t.sendToSrc(1)            
        	}
        	else if (t.dstAttr == 1){
          		t.sendToSrc(-1)
          		t.sendToDst(1)
        	}
        	else{
          		t.sendToSrc(0)
          		t.sendToDst(0)
        	}
      	}), ((a, b) => {
        	if (a != 0) 	a
		else	b
	}) )

    	val g_in_complete = Graph(no_neighbors, g_in.edges)
    
	g_in = g_in_complete

    	remaining_vertices = g_in.vertices.map(x => {
    		if (x._2 == 0)	1
		else	0
	}).collect.sum
     }

    return g_in

  }


  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
	// To Implement
  
	val reverse = Graph(g_in.vertices,g_in.edges.reverse)

    	val bidirect = Graph(g_in.vertices.union(reverse.vertices),g_in.edges.union(reverse.edges))

    	val message = bidirect.aggregateMessages[(Int,Int)](t=>{
        if(t.srcAttr == 1 && t.dstAttr == 1)	t.sendToDst((1,1))
   
	else if(t.srcAttr == 1 && t.dstAttr == -1)	t.sendToDst((1,-1))
   
	else if(t.srcAttr == -1 && t.dstAttr == 1)	t.sendToDst((-1,1))
   
	else val.sendToDst((-1,-1))},

     		(x1,x2) => (math.max(x1._1,x2._1),math.max(x1._2,x2._2))
   	)

	answer = message.map(x => x._2._1+x._2._2).filter(v=>math.abs(v)>0).count()==0

	return answer
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} ).filter({case Edge(a,b,c) => a!=b})
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans)
        println("Yes")
      else
        println("No")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}
