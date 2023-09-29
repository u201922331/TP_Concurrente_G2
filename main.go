package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Contenido de cada fila
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

// Conversión a int (más sencilla)
func toInt(str string) int {
	result, err := strconv.Atoi(str)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	return result
}

// Fácil lectura del CSV seleccionado
func ReadCSV(filename string) []Registro {
	file, err := os.Open(filename)

	registros := []Registro{}

	if err != nil {
		fmt.Println(err.Error())
		return registros
	}

	defer file.Close()

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

func toString(r Registro) string {
	var s string
	s += "{"
	s += strconv.Itoa(r.mm) + "/" + strconv.Itoa(r.yyyy) + " | "
	s += r.region + " | "
	s += r.provincia + " | "
	s += r.ubigeo_distrito + " | "
	s += r.distrito + " | "
	s += r.cod_unidad_ejecutora + " | "
	s += r.desc_unidad_ejecutora + " | "
	s += r.cod_ipress + " | "
	s += r.ipress + " | "
	s += r.nivel_eess + " | "
	s += r.plan_de_seguro + " | "
	s += r.cod_servicio + " | "
	s += r.desc_servicio + " | "
	s += r.sexo + " | "
	s += r.grupo_edad + " | "
	s += strconv.Itoa(r.atenciones)
	s += "}"
	return s
}

// Agregado soporte a Templates/Generics
type Sortables interface {
	int | int16 | int32 | float32 | float64 | string | Registro
}

// Combinar los dos arreglos ingresados y de manera ordenada
func Merge[T Sortables](a []T, b []T, p func(T, T, bool) bool, asc bool) []T {
	var c []T
	i, j := 0, 0

	var m sync.Mutex // mutex para la sincroinzación segura

	for i < len(a) && j < len(b) {
		if p(a[i], b[j], asc) {
			c = append(c, a[i])
			i++
		} else {
			c = append(c, b[j])
			j++
		}
	}

	// Bloquear el acceso concurrente a c
	m.Lock()
	defer m.Unlock()

	for ; i < len(a); i++ {
		c = append(c, a[i])
	}
	for ; j < len(b); j++ {
		c = append(c, b[j])
	}

	return c
}

const (
	SIZE_THRESHOLD = (1 << 11)
)

// Punto de entrada. El segundo parámetro sirve para personalizar la función de criterio de ordenamiento
func MergeSort[T Sortables](arr []T, p func(T, T, bool) bool, asc bool) []T {
	if len(arr) <= 1 {
		return arr
	}

	half := len(arr) / 2
	var l, r []T

	if len(arr) <= SIZE_THRESHOLD { // Secuencial
		l = MergeSort(arr[:half], p, asc)
		r = MergeSort(arr[half:], p, asc)
	} else { // Paralelo
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			l = MergeSort(arr[:half], p, asc)
		}()

		go func() {
			defer wg.Done()
			r = MergeSort(arr[half:], p, asc)
		}()

		wg.Wait()
	}

	return Merge(l, r, p, asc)
}

// Funciones de ordenamiento

func SortByDate(a Registro, b Registro, asc bool) bool {
	if asc {
		if a.yyyy < b.yyyy {
			return true
		}
		if a.yyyy == b.yyyy && a.mm < b.mm {
			return true
		}
	} else {
		if a.yyyy > b.yyyy {
			return true
		}
		if a.yyyy == b.yyyy && a.mm > b.mm {
			return true
		}
	}

	return false
}

func SortByCodIPRESS(a Registro, b Registro, asc bool) bool {
	if asc {
		return a.cod_ipress < b.cod_ipress
	} else {
		return a.cod_ipress > b.cod_ipress
	}
}

func SortByCodUnidadEjecutora(a Registro, b Registro, asc bool) bool {
	if asc {
		return a.cod_unidad_ejecutora < b.cod_unidad_ejecutora
	} else {
		return a.cod_unidad_ejecutora > b.cod_unidad_ejecutora
	}
}

func SortByNumAtenciones(a Registro, b Registro, asc bool) bool {
	if asc {
		return a.atenciones < b.atenciones
	} else {
		return a.atenciones > b.atenciones
	}
}

func PrintRegistros(r []Registro, first int, last int) {
	fmt.Printf("TAMAÑO: %d\n", len(r))
	fmt.Println("ID | FECHA | REGION | PROVINCIA | UBIGEO DISTRITO | COD UNIDAD EJECUTORA | COD IPRESS | IPRESS | NIVEL EESS | PLAN DE SEGURO | COD SERVICIO | DESC SERVICIO | SEXO | GRUPO EDAD | ATENCIONES")
	fmt.Println("================================================================================================================================================================================================")
	for idx, reg := range r[:first] {
		fmt.Println(idx, "->", toString(reg))
	}
	fmt.Print("\n...\n\n")
	for idx, reg := range r[len(r)-last:] {
		fmt.Println(len(r)-last+idx, "->", toString(reg))
	}
	fmt.Println("================================================================================================================================================================================================")
}

// Punto de entrada
func main() {
	registros := ReadCSV("data/data.csv")

	tStart := time.Now()
	reg1 := MergeSort(registros, SortByDate, true)
	t1 := time.Since(tStart)
	PrintRegistros(reg1, 5, 5)
	fmt.Printf("Duración: %s\n\n", t1)

	tStart = time.Now()
	reg2 := MergeSort(registros, SortByCodIPRESS, true)
	t2 := time.Since(tStart)
	PrintRegistros(reg2, 5, 5)
	fmt.Printf("Duración: %s\n\n", t2)

	tStart = time.Now()
	reg3 := MergeSort(registros, SortByCodUnidadEjecutora, true)
	t3 := time.Since(tStart)
	PrintRegistros(reg3, 5, 5)
	fmt.Printf("Duración: %s\n\n", t3)

	tStart = time.Now()
	reg4 := MergeSort(registros, SortByNumAtenciones, true)
	t4 := time.Since(tStart)
	PrintRegistros(reg4, 5, 5)
	fmt.Printf("Duración: %s\n\n", t4)
}
