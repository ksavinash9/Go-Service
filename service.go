package main

import (
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/lib/pq"
	"crypto/md5"
	"github.com/kellydunn/golang-geo"
	"github.com/pierrre/geohash"
	"github.com/weilunwu/go-geofence"
	"gopkg.in/redis.v3"
	"math"
	// "encoding/json"
	// "reflect"
	// "os"
	"strconv"
	"strings"
	"time"
	"io/ioutil"
    "net/http"
    "io"
    "encoding/hex"
)

const MAX_HASH_VARIANT = 2

var MAX_HASH_LENGTH = 12
var HASH_WIDTH_CACHE = make([]float64, MAX_HASH_LENGTH)
var HASH_HEIGHT_CACHE = make([]float64, MAX_HASH_LENGTH)

var GlobalName []string
var GlobalUrlName []string 
var GlobalUuid []string

func set_up_polygon() *geofence.Geofence {
	polygon := []*geo.Point{
		geo.NewPoint(42.01313565896657, -87.89133314508945),
		geo.NewPoint(42.01086525470408, -87.94498134870082),
		geo.NewPoint(41.955566495567936, -87.94566393946297),
		geo.NewPoint(41.937218295745865, -87.88581848144531),
		geo.NewPoint(41.96295962052549, -87.86811594385654),
		geo.NewPoint(41.93385557339662, -87.86333084106445),
		geo.NewPoint(41.934494079111666, -87.81011581420898),
		geo.NewPoint(41.90554916282452, -87.80925750732422),
		geo.NewPoint(41.9058827519221, -87.77938842773438),
		geo.NewPoint(41.86402837073972, -87.77792931126896),
		geo.NewPoint(41.864284053216565, -87.75638580846135),
		geo.NewPoint(41.82348977579423, -87.75552751729265),
		geo.NewPoint(41.823042045417644, -87.80410768697038),
		geo.NewPoint(41.771468158020106, -87.80324938008562),
		geo.NewPoint(41.772364335324305, -87.74625778198242),
		geo.NewPoint(41.730894639311565, -87.74513235432096),
		geo.NewPoint(41.73166805909664, -87.6870346069336),
		geo.NewPoint(41.71748939617332, -87.68600471266836),
		geo.NewPoint(41.716966221614854, -87.7243280201219),
		geo.NewPoint(41.69405798811367, -87.72351264953613),
		geo.NewPoint(41.693865716655395, -87.74385454365984),
		geo.NewPoint(41.67463566843159, -87.74299623677507),
		geo.NewPoint(41.67550471265456, -87.6654052734375),
		geo.NewPoint(41.651683859743336, -87.66489028930664),
		geo.NewPoint(41.65181212480582, -87.64789581298828),
		geo.NewPoint(41.652036588050684, -87.62532234191895),
		geo.NewPoint(41.643100214173714, -87.62506484985352),
		geo.NewPoint(41.643492184875946, -87.51889228820801),
		geo.NewPoint(41.642929165686375, -87.38588330335915),
		geo.NewPoint(41.836600482955916, -87.43858338799328),
		geo.NewPoint(42.05042567111704, -87.40253437310457),
		geo.NewPoint(42.070116505457364, -87.47205723077059),
		geo.NewPoint(42.0681413002819, -87.66792302951217),
		geo.NewPoint(42.02862488227374, -87.66551960259676),
		geo.NewPoint(42.0280511074349, -87.71289814263582),
		geo.NewPoint(41.998468275360544, -87.71301263943315),
		geo.NewPoint(41.9988509912138, -87.75069231167436),
		geo.NewPoint(42.02100207763309, -87.77704238542356),
		geo.NewPoint(42.02010937741473, -87.831029893714),
		geo.NewPoint(41.98719839843826, -87.83120155116194),
		geo.NewPoint(41.9948536336077, -87.86373138340423),
	}
	holes := []*geo.Point{}
	geofence := geofence.NewGeofence([][]*geo.Point{polygon, holes})
	return geofence
}

// geohash_4 := []string{"tdr0", "tdr1", "tdr2", "tdr3", "tdr4", "tdsr", "tdst", "tdsv", "tdsw", "tdsx", "tdsy", "tdsz", "tdu2", "tdu3", "tdu8", "tdu9", "tdub", "te5z", "te7b", "te7c", "te7f", "tehp", "tek0", "tek1", "tepd", "tepe", "tepf", "tepg", "teps", "tg5h", "tg5j", "tg5k", "tgvt", "tgvu", "tgvv", "tgvw", "tgvx", "tgvy", "tgvz", "tgyh", "tgyj", "tgym", "tgyn", "tgyp", "tgyq", "tgyr", "tgyx", "tgyz", "thrj", "thrm", "thrn", "thrp", "thrq", "tsmy", "tsmz", "tsqn", "tsqp", "tsqq", "tsqr", "tstb", "tstc", "tstf", "tsw0", "tsw1", "tsw2", "tsw3", "tsw4", "ttjq", "ttjr", "ttjt", "ttjv", "ttjw", "ttjx", "ttjy", "ttm0", "ttm1", "ttm2", "ttnd", "ttne", "ttnf", "ttng", "ttnj", "ttnm", "ttnn", "ttns", "tuj8", "tuj9", "tujb", "tujc", "tujf", "tun0", "tun1", "tun2", "tun3", "tun4", "tun8"}
// geohash_5 := []string{"tdusb", "tdusc", "tdusf", "tdut0", "tdut1", "tdut2", "tdut3", "tdut4", "thrjh", "thrjj", "thrjk", "thrjm", "thrjn", "thrjs", "thrjt", "thrju", "thrjv", "thrnh", "thrnj", "thrnk", "ttnct", "ttncu", "ttncv", "ttncw", "ttncy", "ttnfh", "ttnfj", "tuqbb", "tuqbc", "tuqc0"}

// geohash_3 := ["123"]
// files := []string{"Test.conf", "util.go", "Makefile", "misc.go", "main.go"}


var test_geofence = set_up_polygon()

type Point struct {
	lat float64
	lon float64
}
type BBox struct {
	top_left_lat    float64
	top_left_lon    float64
	bottom_left_lat float64
	bottom_left_lon float64
}

type Result struct {
	name    	string
	url_name  	string
	uuid  		string
	feature_type string
	latitude float64
	longitude float64
}

type Coverage struct {
	hashes []string
	count  int
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

func main() {
	start := time.Now()
	// queryDB()
	// fmt.Println(len(polygons), len(bboxes))
	var lats,lngs []string

	lata := "77.51215"
	lnga := "12.89443"

	latb := "77.11664"
	lngb := "28.69414"

	lats = append(lats,lata)
	lngs = append(lngs,lnga)
	lats = append(lats,latb)
	lngs = append(lngs,lngb)

	fmt.Println("Display Regions Bulk")
	display_regions_bulk(lats, lngs)
	fmt.Println("Display Regions")
	display_regions(lata,lnga)


	// values := queryDBRent()
	// fmt.Println(values)
	elapsed := time.Since(start)
	fmt.Println( "Time taken = ", elapsed)

}

func setupDB() gorm.DB {
	db, _ := gorm.Open("postgres", "user=avinash password=test dbname=housing_regions sslmode=disable host=10.1.6.123 port=5432")

	db.DB()

	db.DB().Ping()
	db.DB().SetMaxIdleConns(1000000000000000)

	return db
}

func setupRentDB() gorm.DB {
	db, _ := gorm.Open("postgres", "user=avinash password=test dbname=housing sslmode=disable host=10.1.6.126 port=5432")

	db.DB()

	db.DB().Ping()
	db.DB().SetMaxIdleConns(10)

	return db
}

func getContent(url string) ([]byte, error) {
    // Build the request
    // fmt.Println("URL IS +====> ",url)
    req, err := http.NewRequest("GET", url, nil)
    if err != nil {
      return nil, err
    }
    // Send the request via a client
    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
      return nil, err
    }
    // Defer the closing of the body
    defer resp.Body.Close()
    // Read the content into a byte array
    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
      return nil, err
    }
    // At this point we're done - simply return the bytes
    return body, nil
}


func FloatToString(input_num float64) string {
    // to convert a float number to a string
    return strconv.FormatFloat(input_num, 'f', 6, 64)
}

func get_match(first, second string) int {
    i := strings.Index(first, second)
    return i
}

type Cmder interface {
	args() []interface{}
	setErr(error)
	reset()

	writeTimeout() *time.Duration
	readTimeout() *time.Duration
	clusterKey() string

	Err() error
	fmt.Stringer
}

func GetMD5Hash(text string) string {
    hasher := md5.New()
    hasher.Write([]byte(text))
    return hex.EncodeToString(hasher.Sum(nil))
}

func stringInSlice(a string, list []string) bool {
    for _, b := range list {
        if b == a {
            return true
        }
    }
    return false
}

func display_regions_bulk(lats, lngs []string) {
	
	geohash_4 := []string{"tdr0", "tdr1", "tdr2", "tdr3", "tdr4", "tdsr", "tdst", "tdsv", "tdsw", "tdsx", "tdsy", "tdsz", "tdu2", "tdu3", "tdu8", "tdu9", "tdub", "te5z", "te7b", "te7c", "te7f", "tehp", "tek0", "tek1", "tepd", "tepe", "tepf", "tepg", "teps", "tg5h", "tg5j", "tg5k", "tgvt", "tgvu", "tgvv", "tgvw", "tgvx", "tgvy", "tgvz", "tgyh", "tgyj", "tgym", "tgyn", "tgyp", "tgyq", "tgyr", "tgyx", "tgyz", "thrj", "thrm", "thrn", "thrp", "thrq", "tsmy", "tsmz", "tsqn", "tsqp", "tsqq", "tsqr", "tstb", "tstc", "tstf", "tsw0", "tsw1", "tsw2", "tsw3", "tsw4", "ttjq", "ttjr", "ttjt", "ttjv", "ttjw", "ttjx", "ttjy", "ttm0", "ttm1", "ttm2", "ttnd", "ttne", "ttnf", "ttng", "ttnj", "ttnm", "ttnn", "ttns", "tuj8", "tuj9", "tujb", "tujc", "tujf", "tun0", "tun1", "tun2", "tun3", "tun4", "tun8"}
	geohash_5 := []string{"tdusb", "tdusc", "tdusf", "tdut0", "tdut1", "tdut2", "tdut3", "tdut4", "thrjh", "thrjj", "thrjk", "thrjm", "thrjn", "thrjs", "thrjt", "thrju", "thrjv", "thrnh", "thrnj", "thrnk", "ttnct", "ttncu", "ttncv", "ttncw", "ttncy", "ttnfh", "ttnfj", "tuqbb", "tuqbc", "tuqc0"}

	client := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "", // no password set
        DB:       0,  // use default DB
    })

	var latitudes []float64
	var longitudes []float64
	var geohash6 []string
	var results [][]Result
	pipeline := client.Pipeline()

	for i:= 0; i < len(lats); i++ {
		latitude, _ := strconv.ParseFloat(lats[i], 64)
		longitude, _ := strconv.ParseFloat(lngs[i], 64)

		latitudes = append(latitudes,latitude)
		longitudes = append(longitudes,longitude)

		geohash_str6 := geohash.Encode(longitude, latitude, 6)
		geohash_str7 := geohash.Encode(longitude, latitude, 7)

		geohash6 = append(geohash6,geohash_str6)

		pipeline.LRange(geohash_str6,0,-1)
		pipeline.LRange(geohash_str7,0,-1)
	}

	cmds, err := pipeline.Exec()
	if err != nil {
		fmt.Println(err)
	}
	LENGTH := 2*(len(latitudes))


	for index := 0; index < LENGTH;  {
		var current_results []Result
		jindex := index
		for ; jindex < min(index+2,LENGTH); jindex++ {
			result := fmt.Sprintf("%s", cmds[jindex])
			redis_values := strings.Split(result,"&&")

			set := make(map[string]string)
			var feature_types []string
			var uuids []string
			var url_names []string
			var names []string
			var curpolygons []string

			if (jindex%2==0 && stringInSlice(geohash6[jindex/2][0:4],geohash_4)) {
				result_cmd := client.LRange(geohash6[jindex/2][0:4], 0, -1)
				result_4 := fmt.Sprintf("%s", result_cmd)
				redis_values_4 := strings.Split(result_4,"&&")
				redis_values = append(redis_values, redis_values_4...)
			}

			if (jindex%2==0 && stringInSlice(geohash6[jindex/2][0:5],geohash_5)) {
				result_cmd := client.LRange(geohash6[jindex/2][0:5], 0, -1)
				result_5 := fmt.Sprintf("%s", result_cmd)
				redis_values_5 := strings.Split(result_5,"&&")
				redis_values = append(redis_values, redis_values_5...)
			}

			for _,redis_value := range redis_values {
				index := strings.Index(redis_value,"[")
				if index != -1 {
					redis_value = redis_value[index+1:len(redis_value)]
				}
				if len(redis_value) > 10 {
					value_array := strings.Split(redis_value,"::")
					// fmt.Println("Value_Array",value_array)
					md5 := md5.New()
		    		io.WriteString(md5, value_array[2])
					calc_md5 := GetMD5Hash(value_array[2])
					set[calc_md5] = value_array[2] + "--" + value_array[1] + "::" + value_array[3] + "::" + value_array[4] + "::" + value_array[0]
				} else {
						// fmt.Println("Geohash Not Found")
				}
			}

			for _,v := range set {
				set_values := strings.Split(v,"--")
				hash_polygon := set_values[0]
				hash_values := set_values[1]
				var begin,end int
				for i := 0; i < len(hash_polygon); i++ {
					if hash_polygon[i] == '(' {
						begin = i
					} else if '0' <= hash_polygon[i] && hash_polygon[i] <= '9' {
						break 
					}
				}

				for i := len(hash_polygon)-1; i >= 0; i-- {
					if hash_polygon[i] == ')' {
						end = i
					} else if '0' <= hash_polygon[i] && hash_polygon[i] <= '9' {
						break 
					}
				}
				curpolygons = append(curpolygons,hash_polygon[begin+1:end])
				hash_values_array := strings.Split(hash_values,"::")
				feature_types = append(feature_types,hash_values_array[0])
				url_names = append(url_names,hash_values_array[1])
				names = append(names,hash_values_array[2])
				uuids = append(uuids,hash_values_array[3])

			}

			cur_point := geo.NewPoint(latitudes[jindex/2],longitudes[jindex/2])	
			for index,curpolygon := range curpolygons {
				var points []string
				points = strings.Split(curpolygon,",")
				polygon := ConstructPolygonFromPoints(points,uuids[index]=="3b399sdfa9baf4cc8c30e90")
				// fmt.Println("QUERY====>",cur_point,names[index],uuids[index])
				// if uuids[index] == "df1b0d8128f30883c4e5" {
				// // 	fmt.Println("POLYGON======>",curpolygon)
				// }
				if polygon.Contains(cur_point) {
					//If intersection is equal to TRUE
					// fmt.Println("FOUND===>",Result{names[index],url_names[index],uuids[index],feature_types[index],latitudes[jindex/2],longitudes[jindex/2]})
					current_results = append(current_results,Result{names[index],url_names[index],uuids[index],feature_types[index],latitudes[jindex/2],longitudes[jindex/2]})
				} else {
					// IF intersection is equal to FALSE
					// current_results = append(current_results,Result{"Not found this ->" + names[index],"Not found","Not found","Not found",latitudes[jindex/2],longitudes[jindex/2]})
				}
			}
		}
		results = append(results,current_results)
		index = jindex
	}
	fmt.Println(results)
}

func display_regions(lat, lng string) {
	client := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "", // no password set
        DB:       0,  // use default DB
    })

    	
	geohash_4 := []string{"tdr0", "tdr1", "tdr2", "tdr3", "tdr4", "tdsr", "tdst", "tdsv", "tdsw", "tdsx", "tdsy", "tdsz", "tdu2", "tdu3", "tdu8", "tdu9", "tdub", "te5t", "te5v", "te5w", "te5x", "te5y", "te5z", "te78", "te79", "te7b", "te7c", "te7d", "te7e", "te7f", "te7g", "te7s", "te7t", "te7u", "te7v", "te7w", "te7x", "te7y", "te7z", "tee8", "tehj", "tehn", "tehp", "tek0", "tek1", "tek5", "tekh", "tekj", "tepd", "tepe", "tepf", "tepg", "teps", "tg5h", "tg5j", "tg5k", "tgf2", "tgf3", "tgf4", "tgf5", "tgf6", "tgf7", "tgf8", "tgf9", "tgfc", "tgfd", "tgfe", "tgff", "tgvt", "tgvu", "tgvv", "tgvw", "tgvx", "tgvy", "tgvz", "tgyh", "tgyj", "tgym", "tgyn", "tgyp", "tgyq", "tgyr", "tgyx", "tgyz", "thrj", "thrm", "thrn", "thrp", "thrq", "ts5s", "ts5t", "ts5u", "ts5v", "ts5y", "ts5z", "ts7b", "ts7c", "tshj", "tshn", "tshp", "tsk0", "tsmy", "tsmz", "tsqn", "tsqp", "tsqq", "tsqr", "tstb", "tstc", "tstf", "tsw0", "tsw1", "tsw2", "tsw3", "tsw4", "tsyx", "tsyz", "tszp", "tszr", "ttjq", "ttjr", "ttjt", "ttjv", "ttjw", "ttjx", "ttjy", "ttm0", "ttm1", "ttm2", "ttn8", "ttn9", "ttnc", "ttnd", "ttne", "ttnf", "ttng", "ttnj", "ttnm", "ttnn", "ttns", "ttp0", "ttp1", "ttp2", "ttp4", "tuj8", "tuj9", "tujb", "tujc", "tujf", "tun0", "tun1", "tun2", "tun3", "tun4", "tun8"}
	geohash_5 := []string{"tdusb", "tdusc", "tdusf", "tdut0", "tdut1", "tdut2", "tdut3", "tdut4", "thrjh", "thrjj", "thrjk", "thrjm", "thrjn", "thrjs", "thrjt", "thrju", "thrjv", "thrnh", "thrnj", "thrnk", "ttnct", "ttncu", "ttncv", "ttncw", "ttncy", "ttnfh", "ttnfj", "tuqbb", "tuqbc", "tuqc0"}

	latitude, _ := strconv.ParseFloat(lat, 64)
	longitude, _ := strconv.ParseFloat(lng, 64)
	geohash_str7 := geohash.Encode(longitude, latitude, 7)
	geohash_str6 := geohash.Encode(longitude, latitude, 6)
	pipeline := client.Pipeline()
	pipeline.LRange(geohash_str6,0,-1)
	pipeline.LRange(geohash_str7,0,-1)
	cmds, err := pipeline.Exec()
	if err != nil {
		fmt.Println(err)
	}
	
	var results []Result
		
	for _,cmd := range cmds {
		result := fmt.Sprintf("%s", cmd)
		redis_values := strings.Split(result,"&&")

		set := make(map[string]string)
		var feature_types []string
		var uuids []string
		var url_names []string
		var names []string
		var curpolygons []string

		// fmt.Println(stringInSlice(geohash_str6[0:4],geohash_4))
		if (stringInSlice(geohash_str6[0:4],geohash_4)) {
			result_cmd := client.LRange(geohash_str6[0:4], 0, -1)
			result_4 := fmt.Sprintf("%s", result_cmd)
			redis_values_4 := strings.Split(result_4,"&&")
			redis_values = append(redis_values, redis_values_4...)
		}

		if (stringInSlice(geohash_str6[0:5],geohash_5)) {
			result_cmd := client.LRange(geohash_str6[0:5], 0, -1)
			result_5 := fmt.Sprintf("%s", result_cmd)
			redis_values_5 := strings.Split(result_5,"&&")
			redis_values = append(redis_values, redis_values_5...)
		}

		for _,redis_value := range redis_values {
			index := strings.Index(redis_value,"[")
			if index != -1 {
				redis_value = redis_value[index+1:len(redis_value)]
			}
			if len(redis_value) > 10 {
				value_array := strings.Split(redis_value,"::")
				md5 := md5.New()
	    		io.WriteString(md5, value_array[2])
				calc_md5 := GetMD5Hash(value_array[2])
				set[calc_md5] = value_array[2] + "--" + value_array[1] + "::" + value_array[3] + "::" + value_array[4] + "::" + value_array[0]
			} else {
					// fmt.Println("Geohash Not Found")
			}
		}

		for _,v := range set {
			set_values := strings.Split(v,"--")
			hash_polygon := set_values[0]
			hash_values := set_values[1]
			var begin,end int
			for i := 0; i < len(hash_polygon); i++ {
				if hash_polygon[i] == '(' {
					begin = i
				} else if '0' <= hash_polygon[i] && hash_polygon[i] <= '9' {
					break 
				}
			}

			for i := len(hash_polygon)-1; i >= 0; i-- {
				if hash_polygon[i] == ')' {
					end = i
				} else if '0' <= hash_polygon[i] && hash_polygon[i] <= '9' {
					break 
				}
			}
			curpolygons = append(curpolygons,hash_polygon[begin+1:end])
			hash_values_array := strings.Split(hash_values,"::")
			feature_types = append(feature_types,hash_values_array[0])
			url_names = append(url_names,hash_values_array[1])
			names = append(names,hash_values_array[2])
			uuids = append(uuids,hash_values_array[3])

		}

		cur_point := geo.NewPoint(latitude,longitude)	

		for index,curpolygon := range curpolygons {
			var points []string
			points = strings.Split(curpolygon,",")
			polygon := ConstructPolygonFromPoints(points,false)
			if polygon.Contains(cur_point) {
				results = append(results,Result{names[index],url_names[index],uuids[index],feature_types[index],latitude,longitude})
			}
		}
	}
	fmt.Println(results)
}


func queryDBRent() string {
	client := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "", // no password set
        DB:       0,  // use default DB
    })
	db := setupRentDB()
	sql_string2 := `select id,ST_X(coordinates), ST_Y(coordinates) from rent_flats limit 10000`
	defer db.Close()
	db.Exec("set search_path to public, postgis;")
	rows, err := db.Raw(sql_string2).Rows()
	fmt.Println(err)

	db2 := setupDB()
	defer db2.Close()
	db2.Exec("set search_path to public, postgis;")

	match := 0
	not_match := 0
	// total := 0
	var geohash_str7,geohash_str6 string
	var coordinate_x, coordinate_y string
	var flatid int
	var geohashes [MAX_HASH_VARIANT][]string
	var flatids []int
	var lons []float64
	var lats []float64
	for rows.Next() {
		rows.Scan(&flatid, &coordinate_y, &coordinate_x)
		lon, _ := strconv.ParseFloat(coordinate_x, 64)
		lat, _ := strconv.ParseFloat(coordinate_y, 64)
		geohash_str7 = geohash.Encode(lon, lat, 7)
		geohash_str6 = geohash.Encode(lon, lat, 6)
		lats = append(lats,lat)
		lons = append(lons,lon)
		flatids = append(flatids,flatid)
		geohashes[0] = append(geohashes[0],geohash_str7)
		geohashes[1] = append(geohashes[1],geohash_str6)
		// fmt.Println("BeforeGEOHASH",lon,lat,geohash_str)
	}
	fmt.Println("Geohashing")
	// geohashes = append(geohashes,"")
	// for index,geohash := range geohashes[0] {
	// 	ret := client.LRange(geohash,0,-1)
	// 	if len(ret.Val()) == 0 {
	// 		fmt.Println("TESTINGGEOHASH===>",geohash[0],lons[index],lats[index],flatids[index])
	// 		not_match = not_match + 1
	// 	} else {
	// 		match = match + 1
	// 	}
	// 	// fmt.Println("TESTINGGEOHASH===>",geohash,ret,ret.Val())
	// }

	// fmt.Println(geohashes)

	pipeline := client.Pipeline()
	for i := 0; i < MAX_HASH_VARIANT; i++ {
		for _,geohash := range geohashes[i] {
			pipeline.LRange(geohash,0,-1)
		}
	}
	var mod_value = len(geohashes[0])
	fmt.Println(mod_value,geohashes[0][0])
	// pipeline := client.Pipeline()
	// for _, geohash := range geohashes {
	// 	pipeline.LRange(geohash,0,-1)
	// }
	cmds, err := pipeline.Exec()
	// fmt.Println("LENGTH==>",len(cmds))
	var ctr = []int{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}
	var uuids [MAX_HASH_VARIANT][]string
	var names [MAX_HASH_VARIANT][]string
	// fmt.Println("Chutiyaapmistake",mod_value)
	for index,_ := range cmds {
		if index > mod_value/MAX_HASH_VARIANT {
			break
		}

		var polygons [MAX_HASH_VARIANT][]string
		var feature_types [MAX_HASH_VARIANT][]string
		var curuuids [MAX_HASH_VARIANT][]string
		var url_names [MAX_HASH_VARIANT][]string
		var names [MAX_HASH_VARIANT][]string
		flag := 0
		// fmt.Println("INDEX ==> ",index)
		for mod:= 0; mod < MAX_HASH_VARIANT; mod++ {
			// fmt.Println(((mod_value*mod)+index))
			// fmt.Println(cmds[((mod_value*mod)+index)])
			cmd := cmds[(mod_value*mod)+index]
			a := fmt.Sprintf("%b", cmd)
			b := strings.Split(a,"%!b(string=")
			ctr[len(b)] = ctr[len(b)] +1;
			set := make(map[string]string)
			for k:= 3; k < len(b); k++ {
				uuid := b[k][0:20]
				curuuids[mod] = append(curuuids[mod],uuid)
				uuids[mod] = append(uuids[mod],uuid)
				c := strings.Split(b[k],"::")
				// fmt.Println(c)
				calc_md5 := fmt.Sprintf("%x", md5.Sum(([]byte)(c[2])))
				set[calc_md5] = c[2]
				feature_types[mod] = append(feature_types[mod],c[1])
				polygons[mod] = append(polygons[mod],c[2])
				url_names[mod] = append(url_names[mod],c[3])
				names[mod] = append(names[mod],c[4])
			}
			// fmt.Println("CHECK SIZES ===> ", len(polygons[0]),len(polygons[1]),len(names[0]),len(names[1]))
			// fmt.Println("1")
			var curpolygons []string
			for _,v := range polygons[mod] {
				var begin,end int
				for i := 0; i < len(v); i++ {
					if v[i] == '(' {
						begin = i
					} else if '0' <= v[i] && v[i] <= '9' {
						break 
					}
				}

				for i := len(v)-1; i >= 0; i-- {
					if v[i] == ')' {
						end = i
					} else if '0' <= v[i] && v[i] <= '9' {
						break 
					}
				}

				curpolygons = append(curpolygons,v[begin+1:end])
			}
			// fmt.Println("2")
			// fmt.Println("FUCK YOU")
			cur_point := geo.NewPoint(lats[index],lons[index])	
			url := fmt.Sprintf("https://rails.housing.com//api/v2/rent/%d",flatids[index])
			// fmt.Println("URLHIT==>",url)
			content, _ := getContent(url)
			// fmt.Println("API-CONTENT=====>",string(content))
			stringcontent := string(content)
			stringa := "display_regions"
			stringurl := "display_regions_urls"
			stringuuid := "display_regions_uuids"
			// idx:= get_match(stringcontent, stringa)
			// fmt.Println("FOUND INDEXES===>",idx)
			display_regions_index := -1
			display_regions_urls_index := -1
			display_regions_uuids_index := -1
			display_regions_index = strings.Index(stringcontent, stringa)
			display_regions_urls_index = strings.Index(stringcontent, stringurl)
			display_regions_uuids_index = strings.Index(stringcontent, stringuuid)

			fmt.Println(display_regions_index,display_regions_urls_index,display_regions_uuids_index)
			// fmt.Println("LENGTH of current_polygon ===> ",len(curpolygons))
			for i := 0; i < len(curpolygons); i++ {
				var points []string
				points = strings.Split(curpolygons[i],",")
				polygon := ConstructPolygonFromPoints(points,false)
				if polygon.Contains(cur_point) {
					fmt.Println(Result{names[mod][i],url_names[mod][i],uuids[mod][i],feature_types[mod][i],lats[index],lons[index]})
					// fmt.Println("YOLO")
					// fmt.Println("INDEXES ISSUE=====>", mod,index,names,len(names[0]),len(names[1]))
					// fmt.Println("OUTPUT ==> ",strings.Index(string(content), names[mod][i]))
					// fmt.Println("INDEXES ISSUE=====>", mod,index,names)
					if strings.Index(string(content),names[mod][i]) != -1 {
						// fmt.Println("FOUND====>",names[mod][i])
						flag = 1
					} 
				}
			}
			// fmt.Println("CHeck HERE")	
		}
		if flag == 0 {
				not_match = not_match+1
		} else {
			match = match + 1
		}
	}
	fmt.Println("Match and Not Match")
	fmt.Println(match,not_match,len(geohashes))
	// fmt.Println
	fmt.Println("Length of UUID")
	fmt.Println(len(names))


	return "Computed Everything"
}

func queryDB() {
	db := setupDB()

	var sql_string [3]string
	var precision_length [3]int

	precision_length[0] = 7
	precision_length[1] = 6
	precision_length[2] = 5

	sql_string[0] = `SELECT ST_AsText(ST_Transform(polygon, 4326)) as polygon, uuid, ST_AsText(ST_Envelope(ST_Transform(polygon, 4326))) as bbox, feature_type, url_name,name FROM polygons WHERE feature_type = ANY('{39,9939,9938}'::int[])`
	sql_string[1] = `SELECT ST_AsText(ST_Transform(polygon, 4326)) as polygon, uuid, ST_AsText(ST_Envelope(ST_Transform(polygon, 4326))) as bbox, feature_type, url_name,name FROM polygons WHERE feature_type = ANY('{1003,1004,1002,9937,547,37}'::int[])` //547,9937 - 4m|| 9939,39,9938 - 1m30secs | 1003,37 - 
	// sql_string[2] = `SELECT ST_AsText(ST_Transform(polygon, 4326)) as polygon, uuid, ST_AsText(ST_Envelope(ST_Transform(polygon, 4326))) as bbox, feature_type, url_name,name FROM polygons WHERE feature_type = ANY('{37}'::int[])`
	

	defer db.Close()
	db.Exec("set search_path to public, postgis;")
	start := time.Now()
	count := 0

	for index:= 0; index < 2; index++ {
		var polygons []string
		var bboxes []string
		var uuids []string
		var feature_types []string
		var url_names []string
		var names []string
		var polygon, bbox string
		var feature_type string
		var url_name string
		var name string
		var uuid string
		rows, err := db.Raw(sql_string[index]).Rows()
		if err != nil {
			fmt.Println(err)
		}
		for rows.Next() {
			rows.Scan(&polygon, &uuid, &bbox, &feature_type, &url_name, &name)
			polygons = append(polygons, polygon)
			uuids = append(uuids,uuid)
			bboxes = append(bboxes, bbox)
			feature_types = append(feature_types, feature_type)
			url_names = append(url_names,url_name)
			names = append(names,name)
		}
		count = count + compute_bulk_geohash(polygons, uuids, bboxes, feature_types, url_names, names, 9, 2, precision_length[index])
	}

	elapsed := time.Since(start)
	fmt.Println(count, " ", elapsed)

}

func compute_bulk_geohash(polygon_arr []string, uuids []string, bbox_arr []string, feature_types []string, url_names []string, names []string, init_val, end_val, precision int) int {
	count := 0
	var val bool
	counter := 0
	client := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: "", // no password set
        DB:       0,  // use default DB
    })
	for index, bbox := range bbox_arr {
		geoPoly := compute_polygon_from_string(polygon_arr[index])
		bbox_coords := compute_array_points(bbox, init_val, end_val)
		current_uuid := uuids[index]
		current_polygon := polygon_arr[index]
		current_feature_type := feature_types[index]
		current_url_name := url_names[index]
		current_name := names[index]
		bbox_new := compute_bbox(bbox_coords)
		coverage := cover_bounding_box(bbox_new.top_left_lat, bbox_new.top_left_lon, bbox_new.bottom_left_lat, bbox_new.bottom_left_lon, precision)
		count = coverage.count
		var new_bboxes []string
		for _, hash := range coverage.hashes {
			box, _ := geohash.Decode(hash)
			bbox_hash := &BBox{box.Lat.Max, box.Lon.Min, box.Lat.Min, box.Lon.Max}
			val = IntersectsBBox(geoPoly, bbox_hash)
			// fmt.Println(hash)
			if val {
				// fmt.Println(hash)
				redis_value := fmt.Sprintf("%s::%s::%s::%s::%s::&&",current_uuid,current_feature_type,current_polygon,current_url_name,current_name)
				client.RPush(hash, redis_value)
				// client.RPush(current_uuid,hash)
				// fmt.Println(result)
				counter += 1
				new_bboxes = append(new_bboxes, hash)
			}
		}
		coverage.hashes = new_bboxes
		// if current_uuid == "f7f5d7f50dde9452144e" {
		// 	fmt.Println("Precision ==>", current_uuid,precision,coverage.hashes)
		// }
	}
	fmt.Println(counter)
	return count
}

func compute_polygon_from_string(polygon string) *geo.Polygon {
	polygon_coords := compute_array_points(polygon, 9, 2)
	var point_array []*geo.Point
	for _, element := range polygon_coords {
		lon, lat := compute_lon_lat_from_string(element)
		point_array = append(point_array, geo.NewPoint(lat, lon))
	}
	return geo.NewPolygon(point_array)
}

func compute_polygon(polygon_coords []string) []Point {
	polygon := []Point{}
	for _, element := range polygon_coords {
		lon, lat := compute_lon_lat_from_string(element)
		polygon = append(polygon, Point{lat, lon})
	}
	return polygon
}

func compute_bbox(bbox_coords []string) BBox {
	var lat_max, lat_min, lon_max, lon_min float64
	for index, element := range bbox_coords {
		lon, lat := compute_lon_lat_from_string(element)
		if index == 0 {
			lat_max, lon_max = lat, lon
			lat_min, lon_min = lat_max, lon_max
		} else {
			if lat > lat_max {
				lat_min = lat_max
				lat_max = lat
			} else if lat < lat_min {
				lat_max = lat_min
				lat_min = lat
			}
			if lon > lon_max {
				lon_min = lon_max
				lon_max = lon
			} else if lon < lon_min {
				lon_max = lon_min
				lon_min = lon
			}
		}

	}
	return BBox{lat_max, lon_min, lat_min, lon_max}
}

func compute_array_points(polygon string, init_val, end_val int) []string {
	coords_string := polygon[init_val:(len(polygon) - end_val)]
	return strings.Split(coords_string, ",")
}

func compute_lon_lat_from_string(element string) (float64, float64) {
	lon_lat_str := strings.Split(element, " ")
	lon, _ := strconv.ParseFloat(lon_lat_str[0], 64)
	lat, _ := strconv.ParseFloat(lon_lat_str[1], 64)
	return lon, lat
}

func cover_bounding_box(topLeftLat, topLeftLon, bottomRightLat, bottomRightLon float64, precision int) Coverage {
	return cover_bounding_box_max_hashes(topLeftLat, topLeftLon, bottomRightLat, bottomRightLon, 100, precision)
}

func cover_bounding_box_max_hashes(topLeftLat, topLeftLon, bottomRightLat, bottomRightLon float64, max_hashes, precision int) Coverage {
	var coverage Coverage
	startLength := hashLengthToCoverBoundingBox(topLeftLat, topLeftLon, bottomRightLat, bottomRightLon)
	if startLength == 0 {
		startLength = 1
	}
	for length := startLength; length < MAX_HASH_LENGTH; length++ {
		c := coverBoundingBoxLongs(topLeftLat, topLeftLon, bottomRightLat, bottomRightLon, length)
		if length == precision {
			return c
		}
		if c.count > max_hashes {
			if coverage.count == 0 {
				return Coverage{}
			} else {
				return coverage
			}
		} else {
			coverage = c
		}

	}
	return coverage
}

func hashLengthToCoverBoundingBox(topLeftLat, topLeftLon, bottomRightLat, bottomRightLon float64) int {
	isEven := true
	minLat, maxLat := -90.0, 90.0
	minLon, maxLon := -180.0, 180.0
	var mid float64
	for bits := 0; bits < MAX_HASH_LENGTH*5; bits++ {
		if isEven {
			mid = (minLon + maxLon) / 2
			if topLeftLon >= mid {
				if bottomRightLon < mid {
					return bits / 5
				}
				minLon = mid
			} else {
				if bottomRightLon >= mid {
					return bits / 5
				}
				maxLon = mid
			}
		} else {
			mid = (minLat + maxLat) / 2
			if topLeftLat >= mid {
				if bottomRightLat < mid {
					return bits / 5
				}
				minLat = mid
			} else {
				if bottomRightLat >= mid {
					return bits / 5
				}
				maxLat = mid
			}
		}

		isEven = !isEven
	}
	return MAX_HASH_LENGTH
}

func coverBoundingBoxLongs(topLeftLat, topLeftLon, bottomRightLat, bottomRightLon float64, length int) Coverage {
	actualWidthDegreesPerHash := widthDegrees(length)
	actualHeightDegreesPerHash := heightDegrees(length)
	diff := longitudeDiff(bottomRightLon, topLeftLon)
	maxLon := topLeftLon + diff
	var hashes []string

	for lat := bottomRightLat; lat <= topLeftLat; lat += actualHeightDegreesPerHash {
		for lon := topLeftLon; lon <= maxLon; lon += actualWidthDegreesPerHash {
			geohash_str := geohash.Encode(lat, lon, length)
			hashes = append(hashes, geohash_str)
		}
	}
	// ensure have the borders covered
	for lat := bottomRightLat; lat <= topLeftLat; lat += actualHeightDegreesPerHash {
		geohash_str := geohash.Encode(lat, maxLon, length)
		hashes = append(hashes, geohash_str)
	}
	for lon := topLeftLon; lon <= maxLon; lon += actualWidthDegreesPerHash {
		geohash_str := geohash.Encode(topLeftLat, lon, length)
		hashes = append(hashes, geohash_str)
	}
	// ensure that the topRight corner is covered
	geohash_str := geohash.Encode(topLeftLat, maxLon, length)
	hashes = append(hashes, geohash_str)

	return Coverage{hashes, len(hashes)}
}

func longitudeDiff(a, b float64) float64 {
	a = to180(a)
	b = to180(b)
	return math.Abs(to180(a - b))
}

/**
 * Converts an angle in degrees to range -180< x <= 180.
 *
 * @param d
 * @return
 */
func to180(d float64) float64 {
	if d < 0 {
		return -to180(math.Abs(d))
	} else {
		if d > 180 {
			n := Round(math.Floor((d+180)/360.0), 0)
			return d - n*360
		} else {
			return d
		}
	}
}

func Round(x float64, prec int) float64 {
	var rounder float64
	pow := math.Pow(10, float64(prec))
	intermed := x * pow
	_, frac := math.Modf(intermed)
	if frac >= 0.5 {
		rounder = math.Ceil(intermed)
	} else {
		rounder = math.Floor(intermed)
	}

	return rounder / pow
}

func widthDegrees(n int) float64 {
	if n > 0 && n <= MAX_HASH_LENGTH {
		if HASH_WIDTH_CACHE[n-1] == 0.0 {
			HASH_WIDTH_CACHE[n-1] = calculateWidthDegrees(n)
		}
		return HASH_WIDTH_CACHE[n-1]
	} else {
		return calculateWidthDegrees(n)
	}
}

func calculateWidthDegrees(n int) float64 {
	var a float64

	if n%2 == 0 {
		a = -1
	} else {
		a = -0.5
	}
	var result float64
	result = 180 / math.Pow(2, (2.5*float64(n))+a)
	return result
}

func heightDegrees(n int) float64 {
	if n > 0 && n <= MAX_HASH_LENGTH {
		if HASH_HEIGHT_CACHE[n-1] == 0.0 {
			HASH_HEIGHT_CACHE[n-1] = calculateHeightDegrees(n)
		}
		return HASH_HEIGHT_CACHE[n-1]
	} else {
		return calculateHeightDegrees(n)
	}
}

func calculateHeightDegrees(n int) float64 {
	var a float64
	if n%2 == 0 {
		a = 0
	} else {
		a = -0.5
	}
	var result float64
	result = 180 / math.Pow(2, 2.5*float64(n)+a)
	return result
}

func IntersectsBBox(p *geo.Polygon, bbox *BBox) bool {
	top_left := geo.NewPoint(bbox.top_left_lat, bbox.top_left_lon)
	top_right := geo.NewPoint(bbox.top_left_lat, bbox.bottom_left_lon)
	bottom_left := geo.NewPoint(bbox.bottom_left_lat, bbox.bottom_left_lon)
	bottom_right := geo.NewPoint(bbox.bottom_left_lon, bbox.top_left_lon)
	if calc_intersect_with_raycast(p, top_left) {
		return true
	} else if calc_intersect_with_raycast(p, top_right) {
		return true
	} else if calc_intersect_with_raycast(p, bottom_left) {
		return true
	} else if calc_intersect_with_raycast(p, bottom_right) {
		return true
	}

	return false
}

func calc_intersect_with_raycast(p *geo.Polygon, point *geo.Point) bool {
	start := len(p.Points()) - 1
	end := 0
	contains := intersectsWithRaycast(p, point, p.Points()[start], p.Points()[end])

	for i := 1; i < len(p.Points()); i++ {
		if intersectsWithRaycast(p, point, p.Points()[i-1], p.Points()[i]) {
			contains = !contains
		}
	}
	return contains
}

func intersectsWithRaycast(p *geo.Polygon, point *geo.Point, start *geo.Point, end *geo.Point) bool {
	// Always ensure that the the first point
	// has a y coordinate that is less than the second point
	if start.Lng() > end.Lng() {

		// Switch the points if otherwise.
		start, end = end, start

	}

	for point.Lng() == start.Lng() || point.Lng() == end.Lng() {
		newLng := math.Nextafter(point.Lng(), math.Inf(1))
		point = geo.NewPoint(point.Lat(), newLng)
	}

	// If we are outside of the polygon, indicate so.
	if point.Lng() < start.Lng() || point.Lng() > end.Lng() {
		return false
	}

	if start.Lat() > end.Lat() {
		if point.Lat() > start.Lat() {
			return false
		}
		if point.Lat() < end.Lat() {
			return true
		}

	} else {
		if point.Lat() > end.Lat() {
			return false
		}
		if point.Lat() < start.Lat() {
			return true
		}
	}

	raySlope := (point.Lng() - start.Lng()) / (point.Lat() - start.Lat())
	diagSlope := (end.Lng() - start.Lng()) / (end.Lat() - start.Lat())

	return raySlope >= diagSlope
}

func ConstructPolygonFromPoints(points []string, flag bool) geo.Polygon {
	var polygon geo.Polygon
	for index,_ := range points {
		for points[index][0] == '(' {
			points[index] = points[index][1:len(points[index])]
		}
		for points[index][len(points[index])-1] == ')' {
			points[index] = points[index][0:len(points[index])-1]
		}
		if flag {
			fmt.Println(points[index])
		}
		// fmt.Println(points[index])
		point := strings.Split(points[index]," ")
		lon, _ := strconv.ParseFloat(point[0], 64)
		lat, _ := strconv.ParseFloat(point[1], 64)
		new_point := geo.NewPoint(lon,lat)
		polygon.Add(new_point)
	}
	return polygon
}
