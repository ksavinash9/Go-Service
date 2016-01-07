package main

import (
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/lib/pq"
	// "time"
	"bufio"
	"github.com/kellydunn/golang-geo"
	"github.com/pierrre/geohash"
	"github.com/weilunwu/go-geofence"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

var MAX_HASH_LENGTH = 12
var HASH_WIDTH_CACHE = make([]float64, MAX_HASH_LENGTH)
var HASH_HEIGHT_CACHE = make([]float64, MAX_HASH_LENGTH)

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

type Coverage struct {
	hashes []string
	count  int
}

func main() {
	// compute_bulk_geohash_bulk()
	// readLine("/Users/macpro/go_projects/src/github.com/abhijitiitr/es_indexer/res.txt")
	polygons, bboxes := queryDB()
	fmt.Println(len(polygons), len(bboxes))
}

func setupDB() gorm.DB {
	db, _ := gorm.Open("postgres", "user=housing password=housing dbname=housing_regions_production sslmode=disable host=10.1.6.123 port=5432")

	db.DB()

	db.DB().Ping()
	db.DB().SetMaxIdleConns(10)
	// fmt.Println(db)
	// fmt.Println("Hello, playground")

	return db
}

func queryDB() ([]string, []string) {
	db := setupDB()
	sql_string := `SELECT ST_AsText(ST_Transform(polygon, 4326)) as polygon, ST_AsText(ST_Envelope(ST_Transform(polygon, 4326))) as bbox
								 FROM polygons WHERE feature_type = 37 LIMIT 10000`
	defer db.Close()
	db.Exec("set search_path to public, postgis;")
	rows, err := db.Raw(sql_string).Rows()
	fmt.Println(rows.Columns())
	if err != nil {
		fmt.Println(err)
	}
	var polygons []string
	var bboxes []string
	var polygon, bbox string
	for rows.Next() {
		rows.Scan(&polygon, &bbox)
		// fmt.Println(polygon, bbox)
		polygons = append(polygons, polygon)
		bboxes = append(bboxes, bbox)
	}

	start := time.Now()

	count := compute_bulk_geohash(polygons, bboxes, 9, 2, 7)

	elapsed := time.Since(start)
	fmt.Println(count, " ", elapsed)
	return polygons, bboxes
}

func compute_bulk_geohash_bulk() {
	polygon := `POLYGON((75.7800111 26.8229834,75.7800121 26.8229355,75.7800291 26.8228978,75.7818824 26.8191737,75.781953 26.8190961,75.7820527 26.8190482,75.7841255 26.8188328,75.7856296 26.8186907,75.7856658 26.8186907,75.7857027 26.8186939,75.7857233 26.8187307,75.7857233 26.8187704,75.7855686 26.8209823,75.7854467 26.8224614,75.7851571 26.8228731,75.7849334 26.8231284,75.7847601 26.8231986,75.7819706 26.822921,75.7803829 26.8229976,75.7800662 26.8230124,75.7800393 26.8229973,75.7800111 26.8229834))`
	bbox := `POLYGON((75.7800111 26.8186907,75.7800111 26.8231986,75.7857233 26.8231986,75.7857233 26.8186907,75.7800111 26.8186907))`
	bbox_arr := []string{bbox}
	polygon_arr := []string{polygon}
	compute_bulk_geohash(polygon_arr, bbox_arr, 9, 2, 7)
}

func compute_bulk_geohash(polygon_arr []string, bbox_arr []string, init_val, end_val, precision int) int {
	count := 0
	var val bool
	counter := 0

	for index, bbox := range bbox_arr {
		geoPoly := compute_polygon_from_string(polygon_arr[index])
		bbox_coords := compute_array_points(bbox, init_val, end_val)
		bbox_new := compute_bbox(bbox_coords)
		coverage := cover_bounding_box(bbox_new.top_left_lat, bbox_new.top_left_lon, bbox_new.bottom_left_lat, bbox_new.bottom_left_lon, precision)
		count = coverage.count
		var new_bboxes []string
		for _, hash := range coverage.hashes {
			box, _ := geohash.Decode(hash)
			bbox_hash := &BBox{box.Lat.Max, box.Lon.Min, box.Lat.Min, box.Lon.Max}
			val = IntersectsBBox(geoPoly, bbox_hash)
			if val {
				counter += 1
				new_bboxes = append(new_bboxes, hash)
			}
		}
		coverage.hashes = new_bboxes
		// fmt.Println(coverage.hashes)
	}
	fmt.Println(counter, val)
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

	// Move the point's y coordinate
	// outside of the bounds of the testing region
	// so we can start drawing a ray
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

func readLine(path string) {
	inFile, _ := os.Open(path)
	defer inFile.Close()
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	bbox_str := make([]string, 5000)
	count := 0
	for scanner.Scan() {
		bbox_str[count] = scanner.Text()
		count += 1
	}
	start := time.Now()
	polygon := `POLYGON((75.7800111 26.8229834,75.7800121 26.8229355,75.7800291 26.8228978,75.7818824 26.8191737,75.781953 26.8190961,75.7820527 26.8190482,75.7841255 26.8188328,75.7856296 26.8186907,75.7856658 26.8186907,75.7857027 26.8186939,75.7857233 26.8187307,75.7857233 26.8187704,75.7855686 26.8209823,75.7854467 26.8224614,75.7851571 26.8228731,75.7849334 26.8231284,75.7847601 26.8231986,75.7819706 26.822921,75.7803829 26.8229976,75.7800662 26.8230124,75.7800393 26.8229973,75.7800111 26.8229834))`
	polygon_arr := []string{polygon}
	compute_bulk_geohash(polygon_arr, bbox_str, 23, 4, 7)
	elapsed := time.Since(start)
	fmt.Println(count, " ", elapsed)
}
