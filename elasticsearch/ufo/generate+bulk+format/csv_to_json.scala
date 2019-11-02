import com.github.tototoshi.csv._
import java.io._
import java.util.concurrent.atomic.AtomicInteger

val reader = CSVReader.open(new File("./complete.csv")).toStreamWithHeaders

val output = new PrintStream(new FileOutputStream("./complete.es.json"), true)

def getInstrHeader(i : Int) = s"""{ "index": { "_index": "ufo", "_id": $i }}"""

val id = new AtomicInteger(1)

def mapToJson(pl: Map[String,String]) : String = {
	s"""{"datetime" : "${pl("datetime")}","city" : "${pl("city")}","state" : "${pl("state")}","country" : "${pl("country")}","shape" : "${pl("shape")}","duration_in_sec" : "${pl("duration (seconds)")}","duration_in_hrs_mins" : "${pl("duration (hours/min)")}","comments" : "${pl("comments")}","date_posted" :"${pl("date posted")}","location" : "${pl("latitude")},${pl("longitude")}"}"""
}

reader.foreach((item: Map[String,String]) => {
	output.println(getInstrHeader(id.getAndIncrement))
	output.println(mapToJson(item))
})
output.close