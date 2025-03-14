package main

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
//
// 		Chicago Business Intelligence for Strategic Planning Project
//
//		Author: Atef Bader, PhD
//
//
//		The provided source code is NOT the complete implementation for this project
//		The provided source code is for the individual use for students registered in this course
//      	The provided source code can NOT be redistributed
//		The provided source code needs your Google Account geocoder.ApiKey for geocoder.GeocodingReverse
//
//
////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
// The following program will collect data for Taxi Trips, Building permists, and
// Unemployment data from the City of Chicago data portal
// we are using SODA REST API to collect the JSON records
// You coud use the REST API below and post them as URLs in your Browser
// for manual inspection/visualization of data
// the browser will take roughly 5 minutes to get the reply with the JSON data
// and produce the JSON pretty-print
////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// The following is a sample record from the Taxi Trips dataset retrieved from the City of Chicago Data Portal

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// trip_id	"c354c843908537bbf90997917b714f1c63723785"
// trip_start_timestamp	"2021-11-13T22:45:00.000"
// trip_end_timestamp	"2021-11-13T23:00:00.000"
// trip_seconds	"703"
// trip_miles	"6.83"
// pickup_census_tract	"17031840300"
// dropoff_census_tract	"17031081800"
// pickup_community_area	"59"
// dropoff_community_area	"8"
// fare	"27.5"
// tip	"0"
// additional_charges	"1.02"
// trip_total	"28.52"
// shared_trip_authorized	false
// trips_pooled	"1"
// pickup_centroid_latitude	"41.8335178865"
// pickup_centroid_longitude	"-87.6813558293"
// pickup_centroid_location
// type	"Point"
// coordinates
// 		0	-87.6813558293
// 		1	41.8335178865
// dropoff_centroid_latitude	"41.8932163595"
// dropoff_centroid_longitude	"-87.6378442095"
// dropoff_centroid_location
// type	"Point"
// coordinates
// 		0	-87.6378442095
// 		1	41.8932163595
////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"
)

type TaxiTripsJsonRecords []struct {
	Trip_id                    string `json:"trip_id"`
	Trip_start_timestamp       string `json:"trip_start_timestamp"`
	Trip_end_timestamp         string `json:"trip_end_timestamp"`
	Pickup_centroid_latitude   string `json:"pickup_centroid_latitude"`
	Pickup_centroid_longitude  string `json:"pickup_centroid_longitude"`
	Dropoff_centroid_latitude  string `json:"dropoff_centroid_latitude"`
	Dropoff_centroid_longitude string `json:"dropoff_centroid_longitude"`
}

type UnemploymentJsonRecords []struct {
	Community_area                             string `json:"community_area"`
	Community_area_name                        string `json:"community_area_name"`
	Birth_rate                                 string `json:"birth_rate"`
	General_fertility_rate                     string `json:"general_fertility_rate"`
	Low_birth_weight                           string `json:"low_birth_weight"`
	Prenatal_care_beginning_in_first_trimester string `json:"prenatal_care_beginning_in_first_trimester"`
	Preterm_births                             string `json:"preterm_births"`
	Teen_birth_rate                            string `json:"teen_birth_rate"`
	Assault_homicide                           string `json:"assault_homicide"`
	Breast_cancer_in_females                   string `json:"breast_cancer_in_females"`
	Cancer_all_sites                           string `json:"cancer_all_sites"`
	Colorectal_cancer                          string `json:"colorectal_cancer"`
	Diabetes_related                           string `json:"diabetes_related"`
	Firearm_related                            string `json:"firearm_related"`
	Infant_mortality_rate                      string `json:"infant_mortality_rate"`
	Lung_cancer                                string `json:"lung_cancer"`
	Prostate_cancer_in_males                   string `json:"prostate_cancer_in_males"`
	Stroke_cerebrovascular_disease             string `json:"stroke_cerebrovascular_disease"`
	Childhood_blood_lead_level_screening       string `json:"childhood_blood_lead_level_screening"`
	Childhood_lead_poisoning                   string `json:"childhood_lead_poisoning"`
	Gonorrhea_in_females                       string `json:"gonorrhea_in_females"`
	Gonorrhea_in_males                         string `json:"gonorrhea_in_males"`
	Tuberculosis                               string `json:"tuberculosis"`
	Below_poverty_level                        string `json:"below_poverty_level"`
	Crowded_housing                            string `json:"crowded_housing"`
	Dependency                                 string `json:"dependency"`
	No_high_school_diploma                     string `json:"no_high_school_diploma"`
	Per_capita_income                          string `json:"per_capita_income"`
	Unemployment                               string `json:"unemployment"`
}

type BuildingPermitsJsonRecords []struct {
	PermitID         string `json:"id"`
	PermitType       string `json:"permit_type"`
	TotalFee         string `json:"total_fee"`
	Street_number    string `json:"street_number"`
	Street_direction string `json:"street_direction"`
	Street_name      string `json:"street_name"`
	CommunityArea    string `json:"community_area"`
}

type CovidJsonRecords []struct {
	Zip_code                           string `json:"zip_code"`
	Week_number                        string `json:"week_number"`
	Week_start                         string `json:"week_start"`
	Week_end                           string `json:"week_end"`
	Cases_weekly                       string `json:"cases_weekly"`
	Cases_cumulative                   string `json:"cases_cumulative"`
	Case_rate_weekly                   string `json:"case_rate_weekly"`
	Case_rate_cumulative               string `json:"case_rate_cumulative"`
	Percent_tested_positive_weekly     string `json:"percent_tested_positive_weekly"`
	Percent_tested_positive_cumulative string `json:"percent_tested_positive_cumulative"`
	Population                         string `json:"population"`
}

type CCVIJsonRecords []struct {
	Geography_type             string `json:"geography_type"`
	Community_area_or_ZIP_code string `json:"community_area_or_zip"`
	Community_name             string `json:"community_area_name"`
	CCVI_score                 string `json:"ccvi_score"`
	CCVI_category              string `json:"ccvi_category"`
	Location                   struct {
		Coordinates [2]float64 `json:"coordinates"`
	} `json:"location"`
}

func GetZipCode(lat, lon float64) string {
	location := geocoder.Location{Latitude: lat, Longitude: lon}
	addressList, err := geocoder.GeocodingReverse(location)
	if err != nil || len(addressList) == 0 {
		return "" // Handle errors or missing data gracefully
	}
	return addressList[0].PostalCode
}

func GetAirportName(lat, lon float64) string {
	// O'Hare Airport Coordinates
	ohareLat, ohareLon := 41.9803, -87.9090
	// Midway Airport Coordinates
	midwayLat, midwayLon := 41.7868, -87.7522

	// Function to calculate approximate distance
	distance := func(lat1, lon1, lat2, lon2 float64) float64 {
		return (lat1-lat2)*(lat1-lat2) + (lon1-lon2)*(lon1-lon2)
	}

	// Threshold (approximate small radius)
	threshold := 0.0025 // Adjust as needed

	// Check proximity
	if distance(lat, lon, ohareLat, ohareLon) < threshold {
		return "Ohare"
	} else if distance(lat, lon, midwayLat, midwayLon) < threshold {
		return "Midway"
	}

	return "" // Not near an airport

}

func GetLatLonFromAddress(streetNumber, streetDirection, streetName string) (float64, float64, error) {

	geocoder.ApiKey = "AIzaSyD_P6F4hYJk3AY6XkL7gr2mLKSodSqGXp0"

	number, err := strconv.Atoi(streetNumber)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid street number: %s", streetNumber)
	}
	fullAddress := fmt.Sprintf("%s %s %s, Chicago, Illinois, United States", streetNumber, streetDirection, streetName)
	address := geocoder.Address{
		Street:  fmt.Sprintf("%s %s", streetDirection, streetName),
		Number:  number,
		City:    "Chicago",
		State:   "Illinois",
		Country: "United States",
	}

	location, err := geocoder.Geocoding(address)
	if err != nil {
		return 0, 0, fmt.Errorf("error geocoding address %s: %v", fullAddress, err)
	}

	return location.Latitude, location.Longitude, nil
}

// Declare my database connection
var db *sql.DB

// The main package can has the init function.
// The init function will be triggered before the main function

func init() {
	var err error

	fmt.Println("Initializing the DB connection")

	// Establish connection to Postgres Database

	// OPTION 1 - Postgress application running on localhost
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=localhost sslmode=disable port = 5432"

	// OPTION 2
	// Docker container for the Postgres microservice - uncomment when deploy with host.docker.internal
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=host.docker.internal sslmode=disable port = 5433"

	// OPTION 3
	// Docker container for the Postgress microservice - uncomment when deploy with IP address of the container
	// To find your Postgres container IP, use the command with your network name listed in the docker compose file as follows:
	// docker network inspect cbi_backend
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=162.123.0.9 sslmode=disable port = 5433"

	//Option 4
	//Database application running on Google Cloud Platform.
	db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=/cloudsql/chicago-business-intel:us-central1:mypostgres sslmode=disable port = 5432"

	db, err = sql.Open("postgres", db_connection)
	if err != nil {
		log.Fatal(fmt.Println("Couldn't Open Connection to database"))
		panic(err)
	}

	// Test the database connection
	//err = db.Ping()
	//if err != nil {
	//	fmt.Println("Couldn't Connect to database")
	//	panic(err)
	//}

}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func main() {

	// Spin in a loop and pull data from the city of chicago data portal
	// Once every hour, day, week, etc.
	// Though, please note that Not all datasets need to be pulled on daily basis
	// fine-tune the following code-snippet as you see necessary

	// For now while you are doing protyping and unit-testing,
	// it is a good idea to use Cloud Run and start an HTTP server, and manually you kick-start
	// the microservices (goroutines) for data collection from the different sources
	// Once you are done with protyping and unit-testing,
	// you could port your code Cloud Run to  Compute Engine, App Engine, Kubernetes Engine, Google Functions, etc.

	for {

		// While using Cloud Run for instrumenting/prototyping/debugging use the server
		// to trace the state of you running data collection services
		// Navigate to Cloud Run services and find the URL of your service
		// An example of your services URL: https://go-microservice-23zzuv4hksp-uc.a.run.app
		// Use the browser and navigate to your service URL to to kick-start your service

		log.Print("starting CBI Microservices for demo...")

		// Pull the data once a day
		// You might need to pull Taxi Trips and COVID data on daily basis
		// but not the unemployment dataset becasue its dataset doesn't change every day
		// This code snippet is only for prototypying and unit-testing

		// build and fine-tune the functions to pull data from the different data sources
		// The following code snippets show you how to pull data from different data sources

		go GetCommunityAreaUnemployment(db)
		go GetBuildingPermits(db) //TODO: convert coordinates to lat and long then zip code
		go GetTaxiTrips(db)       //all set!
		go GetCovidDetails(db)
		go GetCCVIDetails(db)
		go GetZipCommunityMapping(db)

		http.HandleFunc("/", handler)

		// Determine port for HTTP service.
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
			log.Printf("defaulting to port %s", port)
		}

		// Start HTTP server.
		log.Printf("listening on port %s", port)
		log.Print("Navigate to Cloud Run services and find the URL of your service")
		log.Print("Use the browser and navigate to your service URL to to check your service has started")

		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatal(err)
		}

		time.Sleep(24 * time.Hour)
	}

}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func handler(w http.ResponseWriter, r *http.Request) {
	name := os.Getenv("PROJECT_ID")
	if name == "" {
		name = "CBI-Project"
	}

	fmt.Fprintf(w, "CBI data collection microservices' goroutines have started for %s!\n", name)
}

/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////

func GetTaxiTrips(db *sql.DB) {

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// You need to complete the implmentation and add the data source: https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	// Data Collection needed from two data sources:
	// 1. https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// 2. https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	fmt.Println("GetTaxiTrips: Collecting Taxi Trips Data")

	// Get your geocoder.ApiKey from here :
	// https://developers.google.com/maps/documentation/geocoding/get-api-key?authuser=2

	// geocoder.ApiKey = "AIzaSyD_P6F4hYJk3AY6XkL7gr2mLKSodSqGXp0"
	// "AIzaSyD737jPAyi_Ji947tJFgeRynYBUSRQeTqw"
	geocoder.ApiKey = "AIzaSyD_P6F4hYJk3AY6XkL7gr2mLKSodSqGXp0"
	drop_table := `drop table if exists taxi_trips`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "taxi_trips" (
						"id"   SERIAL , 
						"trip_id" VARCHAR(255) UNIQUE, 
						"trip_start_timestamp" TIMESTAMP WITH TIME ZONE, 
						"trip_end_timestamp" TIMESTAMP WITH TIME ZONE, 
						"pickup_centroid_latitude" DOUBLE PRECISION, 
						"pickup_centroid_longitude" DOUBLE PRECISION, 
						"dropoff_centroid_latitude" DOUBLE PRECISION, 
						"dropoff_centroid_longitude" DOUBLE PRECISION, 
						"pickup_zip_code" VARCHAR(255), 
						"dropoff_zip_code" VARCHAR(255), 
						"pickup_airport" VARCHAR(255),
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Taxi Trips")

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.

	// Get the the Taxi Trips for Taxi medallions list

	var url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=50"

	tr := &http.Transport{
		MaxIdleConns:          10,
		IdleConnTimeout:       1000 * time.Second,
		TLSHandshakeTimeout:   1000 * time.Second,
		ExpectContinueTimeout: 1000 * time.Second,
		DisableCompression:    true,
		Dial: (&net.Dialer{
			Timeout:   1000 * time.Second,
			KeepAlive: 1000 * time.Second,
		}).Dial,
		ResponseHeaderTimeout: 1000 * time.Second,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)

	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Taxi Trips")

	body_1, _ := ioutil.ReadAll(res.Body)
	var taxi_trips_list_1 TaxiTripsJsonRecords
	json.Unmarshal(body_1, &taxi_trips_list_1)

	// Get the Taxi Trip list for rideshare companies like Uber/Lyft list
	// Transportation-Network-Providers-Trips:
	var url_2 = "https://data.cityofchicago.org/resource/m6dm-c72p.json?$limit=50"

	res_2, err := http.Get(url_2)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Transportation-Network-Providers-Trips")

	body_2, _ := ioutil.ReadAll(res_2.Body)
	var taxi_trips_list_2 TaxiTripsJsonRecords
	json.Unmarshal(body_2, &taxi_trips_list_2)

	s := fmt.Sprintf("\n\n Transportation-Network-Providers-Trips number of SODA records received = %d\n\n", len(taxi_trips_list_2))
	io.WriteString(os.Stdout, s)

	// Add the Taxi medallions list & rideshare companies like Uber/Lyft list

	taxi_trips_list := append(taxi_trips_list_1, taxi_trips_list_2...)

	// Process the list

	for i := 0; i < len(taxi_trips_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// There are different methods to deal with messy/dirty/missing data.
		// We will use the simplest method: drop records that have messy/dirty/missing data
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		trip_id := taxi_trips_list[i].Trip_id
		if trip_id == "" {
			continue
		}

		// if trip start/end timestamp doesn't have the length of 23 chars in the format "0000-00-00T00:00:00.000"
		// skip this record

		// get Trip_start_timestamp
		trip_start_timestamp := taxi_trips_list[i].Trip_start_timestamp
		if len(trip_start_timestamp) < 23 {
			continue
		}

		// get Trip_end_timestamp
		trip_end_timestamp := taxi_trips_list[i].Trip_end_timestamp
		if len(trip_end_timestamp) < 23 {
			continue
		}

		pickup_centroid_latitude := taxi_trips_list[i].Pickup_centroid_latitude

		if pickup_centroid_latitude == "" {
			continue
		}

		pickup_centroid_longitude := taxi_trips_list[i].Pickup_centroid_longitude

		if pickup_centroid_longitude == "" {
			continue
		}

		dropoff_centroid_latitude := taxi_trips_list[i].Dropoff_centroid_latitude

		if dropoff_centroid_latitude == "" {
			continue
		}

		dropoff_centroid_longitude := taxi_trips_list[i].Dropoff_centroid_longitude

		if dropoff_centroid_longitude == "" {
			continue
		}

		// Using pickup_centroid_latitude and pickup_centroid_longitude in geocoder.GeocodingReverse
		// we could find the pickup zip-code

		pickup_centroid_latitude_float, _ := strconv.ParseFloat(pickup_centroid_latitude, 64)
		pickup_centroid_longitude_float, _ := strconv.ParseFloat(pickup_centroid_longitude, 64)
		pickup_location := geocoder.Location{
			Latitude:  pickup_centroid_latitude_float,
			Longitude: pickup_centroid_longitude_float,
		}

		// Comment the following line while not unit-testing
		// fmt.Println(pickup_location)

		// pickup_address_list, _ := geocoder.GeocodingReverse(pickup_location)
		// pickup_address := pickup_address_list[0]
		// pickup_zip_code := pickup_address.PostalCode
		var pickup_zip_code string
		pickup_address_list, err := geocoder.GeocodingReverse(pickup_location)
		if err != nil || len(pickup_address_list) == 0 {
			log.Printf("Warning: No address found for pickup location (%f, %f)", pickup_centroid_latitude_float, pickup_centroid_longitude_float)
			pickup_zip_code = "" // Default to empty string if no address found
		} else {
			pickup_address := pickup_address_list[0]
			pickup_zip_code = pickup_address.PostalCode
		}

		if pickup_zip_code == "" {
			continue
		}

		// Using dropoff_centroid_latitude and dropoff_centroid_longitude in geocoder.GeocodingReverse
		// we could find the dropoff zip-code

		dropoff_centroid_latitude_float, _ := strconv.ParseFloat(dropoff_centroid_latitude, 64)
		dropoff_centroid_longitude_float, _ := strconv.ParseFloat(dropoff_centroid_longitude, 64)

		dropoff_location := geocoder.Location{
			Latitude:  dropoff_centroid_latitude_float,
			Longitude: dropoff_centroid_longitude_float,
		}
		// FIXING OUT OF RANGE ISSUE
		var dropoff_zip_code string
		dropoff_address_list, err := geocoder.GeocodingReverse(dropoff_location)
		if err != nil || len(dropoff_address_list) == 0 {
			log.Printf("Warning: No address found for dropoff location (%f, %f)", dropoff_centroid_latitude_float, dropoff_centroid_longitude_float)
			dropoff_zip_code = "" // Default to empty string if no address found
		} else {
			dropoff_address := dropoff_address_list[0]
			dropoff_zip_code = dropoff_address.PostalCode

		}

		if dropoff_zip_code == "" {
			continue
		}

		pickup_airport := GetAirportName(pickup_centroid_latitude_float, pickup_centroid_longitude_float)

		sql := `INSERT INTO taxi_trips ("trip_id", "trip_start_timestamp", "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude", "pickup_zip_code", 
			"dropoff_zip_code", "pickup_airport") values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

		_, err = db.Exec(
			sql,
			trip_id,
			trip_start_timestamp,
			trip_end_timestamp,
			pickup_centroid_latitude,
			pickup_centroid_longitude,
			dropoff_centroid_latitude,
			dropoff_centroid_longitude,
			pickup_zip_code,
			dropoff_zip_code,
			pickup_airport)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the TaxiTrips Table")

}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

func GetCommunityAreaUnemployment(db *sql.DB) {
	fmt.Println("GetCommunityAreaUnemployment: Collecting Unemployment Rates Data")

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Health-Human-Services/Public-Health-Statistics-Selected-public-health-in/iqnk-2tcu/data

	drop_table := `drop table if exists community_area_unemployment`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "community_area_unemployment" (
						"id"   SERIAL , 
						"community_area" VARCHAR(255) UNIQUE, 
						"community_area_name" VARCHAR(255), 
						"birth_rate" VARCHAR(255), 
						"general_fertility_rate" VARCHAR(255), 
						"low_birth_weight" VARCHAR(255),												
						"prenatal_care_beginning_in_first_trimester" VARCHAR(255) , 
						"preterm_births" VARCHAR(255), 
						"teen_birth_rate" VARCHAR(255), 
						"assault_homicide" VARCHAR(255), 
						"breast_cancer_in_females" VARCHAR(255),												
						"cancer_all_sites" VARCHAR(255) , 
						"colorectal_cancer" VARCHAR(255), 
						"diabetes_related" VARCHAR(255), 
						"firearm_related" VARCHAR(255), 
						"infant_mortality_rate" VARCHAR(255),						
						"lung_cancer" VARCHAR(255) , 
						"prostate_cancer_in_males" VARCHAR(255), 
						"stroke_cerebrovascular_disease" VARCHAR(255), 
						"childhood_blood_lead_level_screening" VARCHAR(255), 
						"childhood_lead_poisoning" VARCHAR(255),						
						"gonorrhea_in_females" VARCHAR(255) , 
						"gonorrhea_in_males" VARCHAR(255), 
						"tuberculosis" VARCHAR(255), 
						"below_poverty_level" VARCHAR(255), 
						"crowded_housing" VARCHAR(255),						
						"dependency" VARCHAR(255) , 
						"no_high_school_diploma" VARCHAR(255), 
						"unemployment" VARCHAR(255), 
						"per_capita_income" VARCHAR(255),
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for community_area_unemployment")

	// There are 77 known community areas in the data set
	// So, set limit to 100.
	var url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=100"

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)

	if err != nil {
		panic(err)
	}

	fmt.Println("Community Areas Unemplyment: Received data from SODA REST API for Unemployment")

	body, _ := ioutil.ReadAll(res.Body)
	var unemployment_data_list UnemploymentJsonRecords
	json.Unmarshal(body, &unemployment_data_list)

	s := fmt.Sprintf("\n\n Community Areas number of SODA records received = %d\n\n", len(unemployment_data_list))
	io.WriteString(os.Stdout, s)

	for i := 0; i < len(unemployment_data_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// There are different methods to deal with messy/dirty/missing data.
		// We will use the simplest method: drop records that have messy/dirty/missing data
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		community_area := unemployment_data_list[i].Community_area
		if community_area == "" {
			continue
		}

		community_area_name := unemployment_data_list[i].Community_area_name
		if community_area_name == "" {
			continue
		}

		birth_rate := unemployment_data_list[i].Birth_rate

		general_fertility_rate := unemployment_data_list[i].General_fertility_rate

		low_birth_weight := unemployment_data_list[i].Low_birth_weight

		prenatal_care_beginning_in_first_trimester := unemployment_data_list[i].Prenatal_care_beginning_in_first_trimester

		preterm_births := unemployment_data_list[i].Preterm_births

		teen_birth_rate := unemployment_data_list[i].Teen_birth_rate

		assault_homicide := unemployment_data_list[i].Assault_homicide

		breast_cancer_in_females := unemployment_data_list[i].Breast_cancer_in_females

		cancer_all_sites := unemployment_data_list[i].Cancer_all_sites

		colorectal_cancer := unemployment_data_list[i].Colorectal_cancer

		diabetes_related := unemployment_data_list[i].Diabetes_related

		firearm_related := unemployment_data_list[i].Firearm_related

		infant_mortality_rate := unemployment_data_list[i].Infant_mortality_rate

		lung_cancer := unemployment_data_list[i].Lung_cancer

		prostate_cancer_in_males := unemployment_data_list[i].Prostate_cancer_in_males

		stroke_cerebrovascular_disease := unemployment_data_list[i].Stroke_cerebrovascular_disease

		childhood_blood_lead_level_screening := unemployment_data_list[i].Childhood_blood_lead_level_screening

		childhood_lead_poisoning := unemployment_data_list[i].Childhood_lead_poisoning

		gonorrhea_in_females := unemployment_data_list[i].Gonorrhea_in_females

		gonorrhea_in_males := unemployment_data_list[i].Gonorrhea_in_males

		tuberculosis := unemployment_data_list[i].Tuberculosis

		below_poverty_level := unemployment_data_list[i].Below_poverty_level

		crowded_housing := unemployment_data_list[i].Crowded_housing

		dependency := unemployment_data_list[i].Dependency

		no_high_school_diploma := unemployment_data_list[i].No_high_school_diploma

		per_capita_income := unemployment_data_list[i].Per_capita_income

		unemployment := unemployment_data_list[i].Unemployment

		sql := `INSERT INTO community_area_unemployment ("community_area" , 
		"community_area_name" , 
		"birth_rate" , 
		"general_fertility_rate" , 
		"low_birth_weight" ,
		"prenatal_care_beginning_in_first_trimester" , 
		"preterm_births" , 
		"teen_birth_rate" , 
		"assault_homicide" , 
		"breast_cancer_in_females" ,
		"cancer_all_sites"  , 
		"colorectal_cancer" , 
		"diabetes_related" , 
		"firearm_related" , 
		"infant_mortality_rate" ,
		"lung_cancer" , 
		"prostate_cancer_in_males" , 
		"stroke_cerebrovascular_disease" , 
		"childhood_blood_lead_level_screening" , 
		"childhood_lead_poisoning" ,		
		"gonorrhea_in_females"  , 
		"gonorrhea_in_males" , 
		"tuberculosis" , 
		"below_poverty_level" , 
		"crowded_housing" ,		
		"dependency"  , 
		"no_high_school_diploma" , 
		"unemployment" , 
		"per_capita_income" )
		values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11, $12, $13, $14, $15,$16, $17, $18, $19, $20,$21, $22, $23, $24, $25,$26, $27, $28, $29)`

		_, err = db.Exec(
			sql,
			community_area,
			community_area_name,
			birth_rate,
			general_fertility_rate,
			low_birth_weight,
			prenatal_care_beginning_in_first_trimester,
			preterm_births,
			teen_birth_rate,
			assault_homicide,
			breast_cancer_in_females,
			cancer_all_sites,
			colorectal_cancer,
			diabetes_related,
			firearm_related,
			infant_mortality_rate,
			lung_cancer,
			prostate_cancer_in_males,
			stroke_cerebrovascular_disease,
			childhood_blood_lead_level_screening,
			childhood_lead_poisoning,
			gonorrhea_in_females,
			gonorrhea_in_males,
			tuberculosis,
			below_poverty_level,
			crowded_housing,
			dependency,
			no_high_school_diploma,
			unemployment,
			per_capita_income)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the community_area_unemployment Table")

}

////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////

func GetBuildingPermits(db *sql.DB) {
	fmt.Println("GetBuildingPermits: Collecting Building Permits Data")

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Buildings/Building-Permits/ydr8-5enu/data

	// Data Collection needed from data source:
	// https://data.cityofchicago.org/Buildings/Building-Permits/ydr8-5enu/data

	drop_table := `drop table if exists building_permits`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "building_permits" (
		"id" SERIAL, 
		"permit_id" VARCHAR(255),  
		"permit_code" VARCHAR(255),  
		"permit_type" VARCHAR(255),  
		"total_fee" VARCHAR(255),
		"community_area" VARCHAR(255), 
		street_number VARCHAR(255),
		street_direction VARCHAR(255),
		street_name VARCHAR(255),
		latitude DOUBLE PRECISION,
		longitude DOUBLE PRECISION,
		"zipcode" VARCHAR(255),
		
		PRIMARY KEY ("id") 
	);`
	//add zip code
	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Building Permits")

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var url = "https://data.cityofchicago.org/resource/ydr8-5enu.json?$limit=50"
	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}
	// client := &http.Client{Timeout: 300 * time.Second}

	res, err := client.Get(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Building Permits")

	body, _ := ioutil.ReadAll(res.Body)
	fmt.Println("Building Permits: Received data from SODA REST API for Building Permits")
	// fmt.Println(string(body))

	var building_data_list BuildingPermitsJsonRecords
	// fmt.Println("Building Permits: Unmarshalling JSON data")
	// fmt.Println("Building Permits: Number of records received = ", len(building_data_list))
	json.Unmarshal(body, &building_data_list)
	// fmt.Println("Building Permits: Unmarshalling JSON data completed")
	// fmt.Println("Building Permits: Number of records received after unmarshal= ", len(building_data_list))
	// print first 5 records
	// fmt.Println("Building Permits: First 5 records received after unmarshal= ", building_data_list[:5])

	s := fmt.Sprintf("\n\n Building Permits: number of SODA records received... = %d\n\n", len(building_data_list))
	io.WriteString(os.Stdout, s)

	for i := 0; i < len(building_data_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// There are different methods to deal with messy/dirty/missing data.
		// We will use the simplest method: drop records that have messy/dirty/missing data
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table
		// fmt.Println("Building Permits: Processing Record Number: ", i)
		// fmt.Println("Building Permits: Processing Record Number: ", building_data_list[i])
		permit_id := building_data_list[i].PermitID
		if permit_id == "" {
			continue
		}

		permit_type := building_data_list[i].PermitType
		if permit_type == "" {
			continue
		}

		total_fee := building_data_list[i].TotalFee
		if total_fee == "" {
			continue
		}

		community_area := building_data_list[i].CommunityArea
		if community_area == "" {
			continue
		}

		streetNumber := building_data_list[i].Street_number
		if streetNumber == "" {
			continue
		}

		streetDirection := building_data_list[i].Street_direction
		if streetDirection == "" {
			continue
		}

		streetName := building_data_list[i].Street_name
		if streetName == "" {
			continue
		}

		latitude, longitude, err := GetLatLonFromAddress(streetNumber, streetDirection, streetName)
		if err != nil {
			fmt.Println("Error getting Lat/Lon:", err)
			continue
		}

		zipCode := GetZipCode(latitude, longitude)

		sql := `INSERT INTO building_permits ("permit_id", "permit_type",  "total_fee", "latitude", "longitude",  "community_area", "street_number", "street_direction", "street_name" ,"zipcode") values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

		_, err = db.Exec(
			sql,
			permit_id,
			permit_type,
			total_fee,
			latitude,
			longitude,
			community_area,
			streetNumber,
			streetDirection,
			streetName,
			zipCode)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the Building Permits Table")
	// number of rows inserted into the building_permits table
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
//Sample dataset reviewed:
//"zip_code":"60602",
//"week_number":"35",
//"week_start":"2021-08-29T00:00:00.000",
//"week_end":"2021-09-04T00:00:00.000",
//"cases_weekly":"2",
//"cases_cumulative":"123",
//"case_rate_weekly":"160.8",
//"case_rate_cumulative":"9887.5",
//"tests_weekly":"92",
//"tests_cumulative":"3970",
//"test_rate_weekly":"7395.5",
//"test_rate_cumulative":"319131.8",
//"percent_tested_positive_weekly":"0.022",
//"percent_tested_positive_cumulative":"0.035",
//"deaths_weekly":"0",
//"deaths_cumulative":"2",
//"death_rate_weekly":"0",
//"death_rate_cumulative":"160.8",
//"population":"1244",
//"row_id":"60602-2021-35",
//"zip_code_location":{"type":"Point",
//						"coordinates":
//							0 -87.628309
//							1  41.883136
//":@computed_region_rpca_8um6":"41",
//":@computed_region_vrxf_vc4k":"38",
//":@computed_region_6mkv_f3dw":"14310",
//":@computed_region_bdys_3d7i":"92",
//":@computed_region_43wa_7qmu":"36"
////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

func GetCovidDetails(db *sql.DB) {

	fmt.Println("GetCovidDetails: Collecting Covid Data")

	// create table
	drop_table := `drop table if exists covid_data`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "covid_data" (
						"id"   SERIAL ,
						"zip_code" VARCHAR(255),
						"week_number" VARCHAR(255),
						"week_start" TIMESTAMP WITH TIME ZONE,
						"week_end" TIMESTAMP WITH TIME ZONE,
						"cases_weekly" VARCHAR(255),
						"cases_cumulative" VARCHAR(255),
						"case_rate_weekly" VARCHAR(255),
						"case_rate_cumulative" VARCHAR(255),
						"percent_tested_positive_weekly" VARCHAR(255),
						"percent_tested_positive_cumulative" VARCHAR(255),
						"population" VARCHAR(255),
						PRIMARY KEY ("id")
					);`
	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Covid Data")

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var url = "https://data.cityofchicago.org/resource/yhhz-zm2v.json?$limit=50"

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for COVID")

	body, _ := ioutil.ReadAll(res.Body)
	var covid_data_list CovidJsonRecords
	json.Unmarshal(body, &covid_data_list)

	s := fmt.Sprintf("\n\n COVID: number of SODA records received = %d\n\n", len(covid_data_list))
	io.WriteString(os.Stdout, s)

	for i := 0; i < len(covid_data_list); i++ {

		zip_code := covid_data_list[i].Zip_code
		if zip_code == "" {
			continue
		}

		week_number := covid_data_list[i].Week_number
		if week_number == "" {
			continue
		}

		week_start := covid_data_list[i].Week_start
		if week_start == "" {
			continue
		}

		week_end := covid_data_list[i].Week_end
		if week_end == "" {
			continue
		}

		cases_weekly := covid_data_list[i].Cases_weekly
		if cases_weekly == "" {
			continue
		}

		cases_cumulative := covid_data_list[i].Cases_cumulative
		if cases_cumulative == "" {
			continue
		}

		case_rate_weekly := covid_data_list[i].Case_rate_weekly
		if case_rate_weekly == "" {
			continue
		}

		case_rate_cumulative := covid_data_list[i].Case_rate_cumulative
		if case_rate_cumulative == "" {
			continue
		}

		percent_tested_positive_weekly := covid_data_list[i].Percent_tested_positive_weekly
		if percent_tested_positive_weekly == "" {
			continue
		}

		percent_tested_positive_cumulative := covid_data_list[i].Percent_tested_positive_cumulative
		if percent_tested_positive_cumulative == "" {
			continue
		}

		population := covid_data_list[i].Population
		if population == "" {
			continue
		}

		sql := `INSERT INTO covid_data ("zip_code", "week_number", "week_start", "week_end", "cases_weekly", "cases_cumulative", "case_rate_weekly", "case_rate_cumulative", "percent_tested_positive_weekly", "percent_tested_positive_cumulative", "population") values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10
		, $11)`

		_, err = db.Exec(
			sql,
			zip_code,
			week_number,
			week_start,
			week_end,
			cases_weekly,
			cases_cumulative,
			case_rate_weekly,
			case_rate_cumulative,
			percent_tested_positive_weekly,
			percent_tested_positive_cumulative,
			population)

		if err != nil {
			panic(err)
		}
	}

}

// //////////////////////////////////////////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////////////////////
// Sample dataset reviewed:
// "geography_type":"CA",
// "community_area_or_zip":"70",
// "community_area_name":"Ashburn",
// "ccvi_score":"45.1",
// "ccvi_category":"MEDIUM",
// "rank_socioeconomic_status":"34",
// "rank_household_composition":"32",
// "rank_adults_no_pcp":"28",
// "rank_cumulative_mobility_ratio":"45",
// "rank_frontline_essential_workers":"48",
// "rank_age_65_plus":"29",
// "rank_comorbid_conditions":"33",
// "rank_covid_19_incidence_rate":"59",
// "rank_covid_19_hospital_admission_rate":"66",
// "rank_covid_19_crude_mortality_rate":"39",
// "location":{"type":"Point",
//
//	"coordinates":
//			0	-87.7083657043
//			1	41.7457577128
//
// ":@computed_region_rpca_8um6":"8",
// ":@computed_region_vrxf_vc4k":"69",
// ":@computed_region_6mkv_f3dw":"4300",
// ":@computed_region_bdys_3d7i":"199",
// ":@computed_region_43wa_7qmu":"30"
// //////////////////////////////////////////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////////////////////
func GetCCVIDetails(db *sql.DB) {

	fmt.Println("GetCCVIDetails: Collecting CCVI Data")

	// Drop existing table
	drop_table := `drop table if exists ccvi_data`
	_, err := db.Exec(drop_table)
	if err != nil {
		log.Fatalf("Error dropping ccvi_data table: %v", err)
	}

	// Create table
	create_table := `CREATE TABLE IF NOT EXISTS "ccvi_data" (
						"id" SERIAL PRIMARY KEY,
						"geography_type" VARCHAR(255),
						"community_area_or_zip" VARCHAR(255),
						"community_area_name" VARCHAR(255),
						"ccvi_score" VARCHAR(255),
						"ccvi_category" VARCHAR(255),
						"latitude" DOUBLE PRECISION,
						"longitude" DOUBLE PRECISION,
						"zip_code" VARCHAR(255)
					);`

	_, err = db.Exec(create_table)
	if err != nil {
		log.Fatalf("Error creating ccvi_data table: %v", err)
	}

	fmt.Println("Created Table for CCVI Data")

	// Fetch data
	var url = "https://data.cityofchicago.org/resource/xhc6-88s9.json"

	tr := &http.Transport{
		MaxIdleConns:          10,
		IdleConnTimeout:       1000 * time.Second,
		TLSHandshakeTimeout:   1000 * time.Second,
		ExpectContinueTimeout: 1000 * time.Second,
		DisableCompression:    true,
		Dial: (&net.Dialer{
			Timeout:   1000 * time.Second,
			KeepAlive: 1000 * time.Second,
		}).Dial,
		ResponseHeaderTimeout: 1000 * time.Second,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)

	if err != nil {
		panic(err)
	}

	// Parse JSON
	body, _ := ioutil.ReadAll(res.Body)
	var ccvi_data_list CCVIJsonRecords
	err = json.Unmarshal(body, &ccvi_data_list)
	if err != nil {
		log.Fatalf("Error unmarshalling CCVI data: %v", err)
	}

	fmt.Printf("CCVI records received: %d\n", len(ccvi_data_list))

	// Process data
	for i := 0; i < len(ccvi_data_list); i++ {
		latitude := ccvi_data_list[i].Location.Coordinates[1]
		longitude := ccvi_data_list[i].Location.Coordinates[0]

		// Check valid latitude and longitude
		if latitude == 0.0 || longitude == 0.0 {
			continue
		}

		// Get zip code
		zip_code := GetZipCode(latitude, longitude)
		if zip_code == "" {
			continue
		}

		// Insert data
		sql := `INSERT INTO ccvi_data (
			"geography_type", "community_area_or_zip", "community_area_name", 
			"ccvi_score", "ccvi_category", "latitude", "longitude", "zip_code") 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

		_, err = db.Exec(sql,
			ccvi_data_list[i].Geography_type,
			ccvi_data_list[i].Community_area_or_ZIP_code,
			ccvi_data_list[i].Community_name,
			ccvi_data_list[i].CCVI_score,
			ccvi_data_list[i].CCVI_category,
			latitude,
			longitude,
			zip_code)

		if err != nil {
			fmt.Printf("Error inserting CCVI record: %v", err)
		}
	}
	fmt.Println("Completed inserting CCVI data")

}

// Mapping of ZIP codes to Community Names
var zipToCommunity = map[string]string{
	"60601": "Loop",
	"60602": "Loop",
	"60603": "Loop",
	"60604": "Loop",
	"60605": "Loop, Near South Side",
	"60606": "Loop, Near West Side",
	"60607": "Loop, Near West Side, Near South Side",
	"60608": "Bridgeport, Lower West Side, McKinley Park, Near West Side, North Lawndale, South Lawndale",
	"60609": "Armour Square, Bridgeport, Douglas, Fuller Park, Gage Park, Grand Boulevard, McKinley Park, New City, Washington Park",
	"60610": "Near North Side, Near West Side",
	"60611": "Near North Side",
	"60612": "Near West Side, West Town",
	"60613": "Lake view, North Center, Uptown",
	"60614": "Lincoln Park, Logan Square",
	"60615": "Grand Boulevard, Hyde Park, Kenwood, Washington Park",
	"60616": "Armour Square, Bridgeport, Douglas, Lower West Side, Near South Side",
	"60617": "Avalon Park, Calumet Heights, East Side, South Chicago, South Deering",
	"60618": "Avondale, Irving Park, North Center",
	"60619": "Avalon Park, Burnside, Calumet Heights, Chatham, Greater Grand Crossing, Roseland, South Shore",
	"60620": "Auburn Gresham, Beverly, Chatham, Greater Grand Crossing, Roseland, Washington Heights",
	"60621": "Englewood, Greater Grand Crossing, Washington Park",
	"60622": "Humboldt Park, Logan Square, Near North Side, West Town",
	"60623": "North Lawndale, South Lawndale",
	"60624": "East Garfield Park, Humboldt Park, North Lawndale, West Garfield Park",
	"60625": "Albany Park, Lincoln Square, North Park",
	"60626": "Rogers Park",
	"60627": "Riverdale",
	"60628": "Pullman, Roseland, Washington Heights, West Pullman",
	"60629": "Chicago Lawn, Clearing, Gage Park, Garfield Ridge, West Elsdon, West Lawn",
	"60630": "Albany Park, Forest Glen, Irving Park, Jefferson Park, Portage Park",
	"60631": "Edison Park, Norwood Park",
	"60632": "Archer Heights, Brighton Park, Gage Park, Garfield Ridge, West Elsdon",
	"60633": "Hegewisch, South Deering",
	"60634": "Belmont Cragin, Dunning, Montclare, Portage Park",
	"60635": "Austin, Belmont Cragin, Dunning, Montclare",
	"60636": "Chicago Lawn, Gage Park, West Englewood",
	"60637": "Greater Grand Crossing, Hyde Park, South Shore, Washington Park, Woodlawn",
	"60638": "Clearing, Garfield Ridge",
	"60639": "Austin, Belmont Cragin, Hermosa, Humboldt Park, Logan Square",
	"60640": "Edgewater, Lincoln Square, Uptown",
	"60641": "Avondale, Belmont Cragin, Hermosa, Irving Park, Portage Park",
	"60642": "Beverly",
	"60643": "Beverly, Morgan Park, Washington Heights, West Pullman",
	"60644": "Austin",
	"60645": "West Ridge",
	"60646": "Forest Glen, Jefferson Park, North Park, Norwood Park",
	"60647": "Hermosa, Humboldt Park, Logan Square, West Town",
	"60649": "South Shore",
	"60651": "Austin, Humboldt Park",
	"60652": "Ashburn",
	"60653": "Douglas, Grand Boulevard, Kenwood, Oakland",
	"60655": "Beverly, Morgan Park, Mount Greenwood",
	"60656": "OHare",
	"60657": "Lake view, North Center",
	"60659": "North Park, West Ridge",
	"60660": "Edgewater",
	"60661": "Loop, Near West Side",
	"60664": "Near West Side",
	"60666": "OHare",
	"60680": "Near West Side",
	"60681": "Near West Side",
}

// Fetch community numbers from API and store mappings in PostgreSQL
func GetZipCommunityMapping(db *sql.DB) {
	fmt.Println("Fetching Community Data from API")

	// Drop and create table
	dropTable := `DROP TABLE IF EXISTS zip_community_map`
	_, err := db.Exec(dropTable)
	if err != nil {
		log.Fatal(err)
	}

	createTable := `CREATE TABLE IF NOT EXISTS zip_community_map (
		id SERIAL PRIMARY KEY,
		zip_code VARCHAR(10),
		community_name VARCHAR(255),
		community_number INT
	);`
	_, err = db.Exec(createTable)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Created Table for ZIP Code to Community Mapping")

	// Fetch community data from API
	url := "https://data.cityofchicago.org/resource/igwz-8jzy.json"

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)

	var communityData []struct {
		CommunityName string `json:"community"`
		AreaNumber    string `json:"area_numbe"`
	}

	json.Unmarshal(body, &communityData)
	fmt.Println("Received", len(communityData), "Community Records")

	// Convert API community names to uppercase
	apiCommunityMap := make(map[string]int)
	for _, data := range communityData {
		upperCommunity := strings.ToUpper(data.CommunityName) // Convert to uppercase
		communityNumber, _ := strconv.Atoi(data.AreaNumber)
		apiCommunityMap[upperCommunity] = communityNumber
	}

	// Process ZIP Code mappings
	for zip, communityList := range zipToCommunity {
		communities := strings.Split(communityList, ", ")

		for _, community := range communities {
			// Convert community name to uppercase before lookup
			upperCommunity := strings.ToUpper(community)

			// Fetch correct community number from API response
			communityNumber, exists := apiCommunityMap[upperCommunity]
			if !exists {
				fmt.Printf("Warning: No community number found for %s\n", upperCommunity)
				communityNumber = 0
			}

			// Insert into database (separate row for each community)
			sqlStatement := `INSERT INTO zip_community_map (zip_code, community_name, community_number) VALUES ($1, $2, $3)`
			_, err = db.Exec(sqlStatement, zip, upperCommunity, communityNumber)
			if err != nil {
				fmt.Printf("Error inserting mapping for ZIP %s - Community %s: %v\n", zip, upperCommunity, err)
			}
		}
	}

	fmt.Println("Completed inserting ZIP-Community mappings")
}
