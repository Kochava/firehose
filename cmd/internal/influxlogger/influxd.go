// Copyright 2017 Kochava
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// @Author Ethan Lewis <elewis@kochava.com>

package influxlogger

import (
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

//InfluxD - interface for influx logger
type InfluxD interface {
	PointTickWriter(logSysStats bool) error
	NewInfluxDPoint(name string, tags map[string]string, fields map[string]interface{}, t ...time.Time) (*client.Point, error)
	PushPoint(pt *client.Point)
	//Common point methods
	CreateKafkaPoint(topic string, duration float64)
	CreateRPSPoint(topic string, rps int)
	//add more here
}

//InfluxDImpl is a wrapper around the influx Client that provides hopefully useful functions
type InfluxDImpl struct {
	// Influx http client
	influxCli client.Client
	// Default BatchPoint config
	batchPointConfig client.BatchPointsConfig
	// Batch point container
	batchPoint client.BatchPoints
	// Mutex lock for concurrent read/writes to batchPoints
	batchPointLock sync.Mutex
	// Internal point chan for non-blocking point adds
	pointChan chan *client.Point
}

//ConnectToInflux - return a new clinet connection to influx
func ConnectToInflux(address, username, password string) (client.Client, error) {
	// Create a new HTTPClient
	cli, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     address,
		Username: username,
		Password: password,
		Timeout:  time.Second * 5,
	})

	if err != nil {
		return nil, err
	}
	return cli, nil
}

//NewInfluxD - returns a fully initalized influxD object
func NewInfluxD(cli client.Client, database, precision string) (*InfluxDImpl, error) {
	// reuse
	confg := client.BatchPointsConfig{
		Database:  database,
		Precision: precision,
	}
	bp, err := client.NewBatchPoints(confg)

	if err != nil {
		return nil, err
	}
	return &InfluxDImpl{
		influxCli:        cli,
		batchPoint:       bp,
		batchPointConfig: confg,
		pointChan:        make(chan *client.Point, 1000),
	}, nil
}

//TODO setters for New BP, New BPConfig ... maybe

//PointTickWriter - Main points logger for influxD (lock,deliver,unlock)
func (inf *InfluxDImpl) PointTickWriter(logSysStats bool) error {
	//skip or log system stats (some what pointless)
	if logSysStats {
		go inf.tickSystemStats() //(continuous)
	}
	go inf.createBatchPoints() //locks/unlocks internally (continuous)

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	var err error
	for range ticker.C {
		inf.batchPointLock.Lock()
		//make a copy of the current batch point
		tmpBatchPoint := inf.batchPoint
		// create a new batch point with the default batchpoint config
		inf.batchPoint, err = client.NewBatchPoints(inf.batchPointConfig)
		if err != nil {
			log.Println("InfluxD : Failed to create new batchpoint")
		}
		inf.batchPointLock.Unlock()
		err := inf.writeBatchPoints(tmpBatchPoint)
		if err != nil {
			log.Println("InfluxD : Failed to sending point to influx: ", err)
		}
	}
	return nil
}

//NewInfluxDPoint - wrapper around influx client NewPoint (so our app doesn't need the influx client import directly)
func (inf *InfluxDImpl) NewInfluxDPoint(name string, tags map[string]string, fields map[string]interface{}, t ...time.Time) (*client.Point, error) {
	return client.NewPoint(name, tags, fields, t...)
}

//PushPoint - adds a point to the internal buffered chan
func (inf *InfluxDImpl) PushPoint(pt *client.Point) {
	select {
	case inf.pointChan <- pt: // add to point channel if there is room
	default: // drop the point channel is full
		log.Println("InfluxD : Internal point chan full : ", len(inf.pointChan)) // <- warning saying we are dropping points
		log.Println("InfluxD : attempting to write directly to batch point")
		inf.addToBatchPoints(pt)
	}
}

//CreateKafkaPoint - creates a kafka_stats point metric
func (inf *InfluxDImpl) CreateKafkaPoint(topic string, duration float64) {
	tags := map[string]string{"host": inf.getHostName()}
	fields := map[string]interface{}{}
	tags["topic"] = topic
	fields["duration"] = duration
	pt, err := inf.NewInfluxDPoint("kafka_stats", tags, fields, time.Now())
	if err != nil {
		log.Println("InfluxD : ", err)
	}
	inf.PushPoint(pt)
}

//CreateRPSPoint - creates a kafka_stats point metric
func (inf *InfluxDImpl) CreateRPSPoint(topic string, rps int) {
	tags := map[string]string{"host": inf.getHostName()}
	fields := map[string]interface{}{}
	tags["topic"] = topic
	fields["rps"] = rps
	pt, err := inf.NewInfluxDPoint("kafka_stats", tags, fields, time.Now())
	if err != nil {
		log.Println("InfluxD : ", err)
	}
	inf.PushPoint(pt)
}

//------------------------
// Unexported helper funcs
//------------------------

//addToBatchPoints - adds point to batchPoint (lock,write,unlock)
func (inf *InfluxDImpl) addToBatchPoints(pt *client.Point) {
	inf.batchPointLock.Lock()
	inf.batchPoint.AddPoint(pt)
	inf.batchPointLock.Unlock()
}

//createBatchPoints - pulls from pointchan and adds to internal batch point object
func (inf *InfluxDImpl) createBatchPoints() {
	//this pulls points from the interall point chan
	for pt := range inf.pointChan {
		inf.addToBatchPoints(pt) //locks/unlocks internally
	}
}

// writeBatchPoints - makes a request to write a batchPoint to influx
func (inf *InfluxDImpl) writeBatchPoints(bp client.BatchPoints) error {
	if err := inf.influxCli.Write(bp); err != nil {
		return err
	}
	return nil
}

//GetHostName - Return the name of the host
func (inf *InfluxDImpl) getHostName() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Println(err)
		hostname = "N/A"
	}
	return hostname
}

// tickSystemStats - write a system stats point every 60s
func (inf *InfluxDImpl) tickSystemStats() {
	//get the host name
	host := inf.getHostName()

	log.Printf("tickSystemStats - %s", host)
	// add sys point every 60s
	ticker := time.NewTicker(time.Second * 60)
	defer ticker.Stop()
	for range ticker.C {
		tags := map[string]string{"host": host} //index
		fields := map[string]interface{}{}
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		fields["active_go_routines"] = runtime.NumGoroutine()
		fields["mallocs"] = int64(memStats.Mallocs)
		fields["frees"] = int64(memStats.Frees)
		fields["lookups"] = int64(memStats.Lookups)
		fields["memory_total_alloc"] = int64(memStats.TotalAlloc)
		fields["memory_total_heap_alloc"] = int64(memStats.HeapAlloc)
		fields["memory_total_heap_in_use"] = int64(memStats.HeapInuse)
		fields["memory_total_stack_sys"] = int64(memStats.StackSys)
		fields["memory_total_stack_in_use"] = int64(memStats.StackInuse)
		fields["memory_total_sys"] = int64(memStats.Sys)
		//Create the new influx point
		pt, err := inf.NewInfluxDPoint("system_stats", tags, fields, time.Now())
		if err != nil {
			log.Println("InfluxD : ", err)
		}
		//add the point to the internal chan if not nil
		if pt != nil {
			inf.PushPoint(pt)
		}
	}
}
