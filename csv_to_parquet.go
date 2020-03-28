package main

import (
	//"fmt"
	"log"
	"os"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	//"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type Shoe struct {
	ShoeBrand string `parquet:"name=shoe_brand, type=UTF8, encoding=PLAIN_DICTIONARY"`
	ShoeName  int32  `parquet:"name=shoe_name, type=UTF8, encoding=PLAIN_DICTIONARY"`
}

func main() {
	var err error

	csvfile, err := os.Open("data/shoes.csv")
	if err != nil {
		log.Fatal(err)
	}

	fw, err := local.NewLocalFileWriter("flat.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	//write
	pw, err := writer.NewParquetWriter(fw, new(Shoe), 2)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	// set pw options
	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// I need to iterate over every row in csvfile
	// For each row, I need to create a Shoe
	// then I need to write each shoe with code like this
	//if err = pw.Write(shoe); err != nil {
	//log.Println("Write error", err)
	//}

	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}

	log.Println("Write Finished")
	fw.Close()
}
