package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Registro struct {
	yyyy                  int
	mm                    int
	region                string
	provincia             string
	ubigeo_distrito       string
	distrito              string
	cod_unidad_ejecutora  string
	desc_unidad_ejecutora string
	cod_ipress            string
	ipress                string
	nivel_eess            string
	plan_de_seguro        string
	cod_servicio          string
	desc_servicio         string
	sexo                  string
	grupo_edad            string
	atenciones            int
}

func toInt(str string) int {
	result, _ := strconv.Atoi(str)
	return result
}

func ReadCSV(filename string) []Registro {
	file, err := os.Open(filename)

	if err != nil {
		fmt.Println(err.Error())
		return []Registro{}
	}

	defer file.Close()

	registros := []Registro{}

	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "|")

		if i > 0 {
			registros = append(registros, Registro{
				toInt(fields[0]),
				toInt(fields[1]),
				fields[2],
				fields[3],
				fields[4],
				fields[5],
				fields[6],
				fields[7],
				fields[8],
				fields[9],
				fields[10],
				fields[11],
				fields[12],
				fields[13],
				fields[14],
				fields[15],
				toInt(fields[16]),
			})
		}

		i++
	}
	return registros
}

func main() {
	registros := ReadCSV("test.txt")
	for idx, reg := range registros {
		fmt.Println("REGISTRO ", idx, ": ", reg)
	}
}
