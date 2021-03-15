# 6 Práctica: Personas

1. [Arrancar Zeppelin ](#schema1)
2. [Importación de librerías ](#schema2)
3. [Crear una clase](#schema3)
4. [Crear función para procesar personas](#schema4)
5. [Conexión con SparkSQL](#schema5)







3. [Cargar ficheros ](#schema3)
4. [Procesar la inforamción cargada en RDD](#schema4)
5. [Aplicar "procesarLinea"](#schema5)
6. [Imprimir valores](#schema6)

<hr>

<a name="schema1"></a>

# 1. Arrancar Zeppelin
Navegamos en la consola hasta llegar donde tenemos descargados la carpeta Zeppelin y ejecutamos:
~~~
bin/zeppelin-daemon.sh start
~~~

Seguidamente abrimos un página en el navegador y vamos a `http://localhost:8080`, se nos abre zeppelin y creamos un nuevo notebook, llamado Temperatura Sensor y como intérprete elegimos `spark2`
<hr>

<a name="schema2"></a>

# 2. Importación de librerías

~~~scala
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
~~~


<hr>

<a name="schema3"></a>

# 3. Creamos un clase Persona

~~~scala
case class Persona(ID: Int, nombre:String,edad:Int,numAmigos: Int)
~~~
<hr>

<a name="schema4"></a>

# 4. Crear función para procesar personas

~~~scala
def procesarPersona(linea:String):Persona = {
    val campos = linea.split(",")
    val persona = Persona(campos(0).toInt, campos(1),campos(2).toInt, campos(3).toInt )
    persona
}
~~~

<hr>

<a name="schema5"></a>

# 5. Conexion con SparkSQL
~~~scala
val spark = SparkSession.builder.appName("Personas").getOrCreate()
~~~
