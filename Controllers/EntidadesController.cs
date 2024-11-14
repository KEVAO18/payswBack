#nullable enable // Habilita las características de referencia nula en C#, permitiendo anotaciones y advertencias relacionadas con posibles valores nulos.
using System; // Importa el espacio de nombres que contiene tipos fundamentales como Exception, Console, etc.
using System.Collections.Generic; // Importa el espacio de nombres para colecciones genéricas como Dictionary.
using System.Data; // Importa el espacio de nombres para clases relacionadas con bases de datos.
using System.Data.Common; // Importa el espacio de nombres que define la clase base para proveedores de datos.
using Microsoft.AspNetCore.Authorization; // Importa el espacio de nombres para el control de autorización en ASP.NET Core.
using Microsoft.AspNetCore.Mvc; // Importa el espacio de nombres para la creación de controladores en ASP.NET Core.
using Microsoft.Extensions.Configuration; // Importa el espacio de nombres para acceder a la configuración de la aplicación.
using Microsoft.Data.SqlClient; // Importa el espacio de nombres necesario para trabajar con SQL Server y LocalDB.
using System.Linq; // Importa el espacio de nombres para operaciones de consulta con LINQ.
using System.Text.Json; // Importa el espacio de nombres para manejar JSON.
using ProyectoBackendCsharp.Models; // Importa los modelos del proyecto.
using ProyectoBackendCsharp.Services; // Importa los servicios del proyecto.
using BCrypt.Net;// Importa el espacio de nombres para trabajar con BCrypt para hashing de contraseñas.
using System.Text; 

namespace ProyectoBackendCsharp.Controllers
{
    [Route("api/{projectName}/{tableName}")] // Define la ruta de la API para este controlador.
    [ApiController] // Indica que esta clase es un controlador de API.
    [Authorize] // Requiere autorización para acceder a los métodos de este controlador.
    public class EntidadesController : ControllerBase // Define un controlador llamado `EntidadesController`.
    {
        private readonly ControlConexion controlConexion; // Declara una instancia del servicio ControlConexion.
        private readonly IConfiguration _configuration; // Declara una instancia de la configuración de la aplicación.

        // Constructor que recibe las dependencias necesarias y lanza excepciones si son nulas.
        public EntidadesController(ControlConexion controlConexion, IConfiguration configuration)
        {
            this.controlConexion = controlConexion ?? throw new ArgumentNullException(nameof(controlConexion));
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        [AllowAnonymous] // Permite el acceso anónimo a este método.
        [HttpGet] // Define una ruta HTTP GET para este método.
        public IActionResult Listar(string projectName, string tableName) // Método que lista todas las filas de una tabla dada.
        {
            if (string.IsNullOrWhiteSpace(tableName)) // Verifica si el nombre de la tabla está vacío o solo contiene espacios en blanco.
                return BadRequest("El nombre de la tabla no puede estar vacío."); // Retorna una respuesta de error si la tabla está vacía.

            try
            {
                var lista = new List<Dictionary<string, object?>>(); // Crea una lista para almacenar las filas resultantes.
                string comandoSQL = $"SELECT * FROM {tableName}"; // Define el comando SQL para seleccionar todas las filas de la tabla.

                controlConexion.AbrirBd(); // Abre la conexión a la base de datos.
                var tabla = controlConexion.EjecutarConsultaSql(comandoSQL, null); // Ejecuta la consulta SQL y almacena el resultado en un DataTable.
                controlConexion.CerrarBd(); // Cierra la conexión a la base de datos.

                foreach (DataRow fila in tabla.Rows) // Recorre cada fila en el DataTable.
                {
                    var propiedades = fila.Table.Columns.Cast<DataColumn>()
                                        .ToDictionary(col => col.ColumnName, col => fila[col] == DBNull.Value ? null : fila[col]); // Convierte cada fila en un diccionario.
                    lista.Add(propiedades); // Agrega el diccionario a la lista.
                }

                return Ok(lista); // Retorna la lista de filas en formato JSON.
            }
            catch (Exception ex) // Captura cualquier excepción que ocurra durante la ejecución.
            {
                return StatusCode(500, $"Error interno del servidor: {ex.Message}"); // Retorna una respuesta de error 500 con el mensaje de la excepción.
            }
        }

        [AllowAnonymous] // Permite el acceso anónimo a este método.
        [HttpGet("{keyName}/{value}")] // Define una ruta HTTP GET con parámetros adicionales.
        public IActionResult GetByKey(string projectName, string tableName, string keyName, string value) // Método que obtiene una fila específica basada en una clave.
        {
            if (string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(keyName) || string.IsNullOrWhiteSpace(value)) // Verifica si alguno de los parámetros está vacío.
            {
                return BadRequest("El nombre de la tabla, el nombre de la clave y el valor no pueden estar vacíos."); // Retorna una respuesta de error si algún parámetro está vacío.
            }

            controlConexion.AbrirBd(); // Abre la conexión a la base de datos.
            try
            {
                string provider = _configuration["DatabaseProvider"] ?? throw new InvalidOperationException("DatabaseProvider not configured."); // Obtiene el proveedor de base de datos desde la configuración.
                
                string query;
                DbParameter[] parameters;
                
                // Define la consulta SQL y los parámetros para SQL Server y LocalDB.
                query = "SELECT data_type FROM information_schema.columns WHERE table_name = @tableName AND column_name = @columnName";
                parameters = new DbParameter[]
                {
                    CreateParameter("@tableName", tableName),
                    CreateParameter("@columnName", keyName)
                };

                Console.WriteLine($"Executing SQL query: {query} with parameters: tableName={tableName}, columnName={keyName}");

                var dataTypeResult = controlConexion.EjecutarConsultaSql(query, parameters); // Ejecuta la consulta SQL para determinar el tipo de dato de la clave.

                if (dataTypeResult == null || dataTypeResult.Rows.Count == 0 || dataTypeResult.Rows[0]["data_type"] == DBNull.Value) // Verifica si se obtuvo un resultado válido.
                {
                    return NotFound("No se pudo determinar el tipo de dato."); // Retorna una respuesta de error si no se pudo determinar el tipo de dato.
                }

                string dataType = dataTypeResult.Rows[0]["data_type"]?.ToString() ?? ""; // Obtiene el tipo de dato de la columna.
                Console.WriteLine($"Detected data type for column {keyName}: {dataType}");

                if (string.IsNullOrEmpty(dataType)) // Verifica si el tipo de dato es válido.
                {
                    return NotFound("No se pudo determinar el tipo de dato."); // Retorna una respuesta de error si el tipo de dato es inválido.
                }

                object convertedValue;
                string comandoSQL;

                // Determina cómo tratar el valor y la consulta SQL según el tipo de dato, compatible con SQL Server y LocalDB.
                switch (dataType.ToLower())
                {
                    case "int":
                    case "bigint":
                    case "smallint":
                    case "tinyint":
                        if (int.TryParse(value, out int intValue))
                        {
                            convertedValue = intValue;
                            comandoSQL = $"SELECT * FROM {tableName} WHERE {keyName} = @Value";
                        }
                        else
                        {
                            return BadRequest("El valor proporcionado no es válido para el tipo de datos entero.");
                        }
                        break;
                    case "decimal":
                    case "numeric":
                    case "money":
                    case "smallmoney":
                        if (decimal.TryParse(value, out decimal decimalValue))
                        {
                            convertedValue = decimalValue;
                            comandoSQL = $"SELECT * FROM {tableName} WHERE {keyName} = @Value";
                        }
                        else
                        {
                            return BadRequest("El valor proporcionado no es válido para el tipo de datos decimal.");
                        }
                        break;
                    case "bit":
                        if (bool.TryParse(value, out bool boolValue))
                        {
                            convertedValue = boolValue;
                            comandoSQL = $"SELECT * FROM {tableName} WHERE {keyName} = @Value";
                        }
                        else
                        {
                            return BadRequest("El valor proporcionado no es válido para el tipo de datos booleano.");
                        }
                        break;
                    case "float":
                    case "real":
                        if (double.TryParse(value, out double doubleValue))
                        {
                            convertedValue = doubleValue;
                            comandoSQL = $"SELECT * FROM {tableName} WHERE {keyName} = @Value";
                        }
                        else
                        {
                            return BadRequest("El valor proporcionado no es válido para el tipo de datos flotante.");
                        }
                        break;
                    case "nvarchar":
                    case "varchar":
                    case "nchar":
                    case "char":
                    case "text":
                        convertedValue = value;
                        comandoSQL = $"SELECT * FROM {tableName} WHERE {keyName} = @Value";
                        break;
                    case "date":
                    case "datetime":
                    case "datetime2":
                    case "smalldatetime":
                        if (DateTime.TryParse(value, out DateTime dateValue))
                        {
                            comandoSQL = $"SELECT * FROM {tableName} WHERE CAST({keyName} AS DATE) = @Value";
                            convertedValue = dateValue.Date;
                        }
                        else
                        {
                            return BadRequest("El valor proporcionado no es válido para el tipo de datos fecha.");
                        }
                        break;
                    default:
                        return BadRequest($"Tipo de dato no soportado: {dataType}"); // Retorna un error si el tipo de dato no es soportado.
                }

                var parametro = CreateParameter("@Value", convertedValue); // Crea el parámetro para la consulta SQL.

                Console.WriteLine($"Executing SQL query: {comandoSQL} with parameter: {parametro.ParameterName} = {parametro.Value}, DbType: {parametro.DbType}");

                var resultado = controlConexion.EjecutarConsultaSql(comandoSQL, new DbParameter[] { parametro }); // Ejecuta la consulta SQL con el parámetro.

                Console.WriteLine($"DataSet fill completed for query: {comandoSQL}");

                if (resultado.Rows.Count > 0) // Verifica si hay filas en el resultado.
                {
                    var lista = new List<Dictionary<string, object?>>();
                    foreach (DataRow fila in resultado.Rows)
                    {
                        var propiedades = resultado.Columns.Cast<DataColumn>()
                                           .ToDictionary(col => col.ColumnName, col => fila[col] == DBNull.Value ? null : fila[col]);
                        lista.Add(propiedades);
                    }

                    return Ok(lista); // Retorna las filas encontradas en formato JSON.
                }

                return NotFound(); // Retorna un error 404 si no se encontraron filas.
            }
            catch (Exception ex) // Captura cualquier excepción que ocurra durante la ejecución.
            {
                Console.WriteLine($"Exception occurred: {ex.Message}");
                return StatusCode(500, $"Error interno del servidor: {ex.Message}"); // Retorna un error 500 si ocurre una excepción.
            }
            finally
            {
                controlConexion.CerrarBd(); // Cierra la conexión a la base de datos.
            }
        }

        //agregado por mi
        [AllowAnonymous] // Permite el acceso anónimo a este método.
        [HttpGet("top{size}")] // Define una ruta HTTP GET para este método.
        public IActionResult GetTop(string tableName, int size)
        {
            if (string.IsNullOrWhiteSpace(tableName) || size <= 0)
            {
                return BadRequest("El nombre de la tabla no puede estar vacío y el tamaño debe ser mayor a cero.");
            }

            controlConexion.AbrirBd();
            try
            {
                // Ajuste: esta consulta solo verifica que la tabla existe; aquí no necesitamos el tipo de dato
                string comandoSQL = $"SELECT TOP {size} * FROM {tableName}";

                Console.WriteLine($"Executing SQL query: {comandoSQL}");

                var resultado = controlConexion.EjecutarConsultaSql(comandoSQL, Array.Empty<DbParameter>());

                if (resultado.Rows.Count > 0)
                {
                    var lista = new List<Dictionary<string, object?>>();
                    foreach (DataRow fila in resultado.Rows)
                    {
                        var propiedades = resultado.Columns.Cast<DataColumn>()
                                            .ToDictionary(col => col.ColumnName, col => fila[col] == DBNull.Value ? null : fila[col]);
                        lista.Add(propiedades);
                    }

                    return Ok(lista); // Retorna todos los registros obtenidos en formato JSON.
                }

                return NotFound(); // Retorna un error 404 si no se encontraron registros.
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occurred: {ex.Message}");
                return StatusCode(500, $"Error interno del servidor: {ex.Message}");
            }
            finally
            {
                controlConexion.CerrarBd();
            }
        }

        //agregado por mi
        [AllowAnonymous] // Permite el acceso anónimo a este método.
        [HttpGet("columns")] // Define una ruta HTTP GET para este método.
        public IActionResult GetTableColumns(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                return BadRequest("El nombre de la tabla no puede estar vacío.");
            }

            controlConexion.AbrirBd();
            try
            {
                // Consulta para obtener las columnas de la tabla especificada
                string query = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @tableName";
                var parameters = new DbParameter[]
                {
            CreateParameter("@tableName", tableName)
                };

                Console.WriteLine($"Executing SQL query: {query} with parameter: tableName = {tableName}");

                var resultado = controlConexion.EjecutarConsultaSql(query, parameters);

                if (resultado.Rows.Count > 0)
                {
                    // Construye el JSON con detalles de cada columna
                    var columnas = resultado.AsEnumerable()
                                            .Select(row => new
                                            {
                                                Nombre = row["COLUMN_NAME"].ToString(),
                                                TipoDato = row["DATA_TYPE"].ToString(),
                                                LongitudMaxima = row["CHARACTER_MAXIMUM_LENGTH"] == DBNull.Value ? null : (int?)row["CHARACTER_MAXIMUM_LENGTH"]
                                            })
                                            .ToList();

                    return Ok(columnas); // Retorna la lista de columnas con detalles en formato JSON.
                }

                return NotFound($"No se encontraron columnas para la tabla {tableName}.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occurred: {ex.Message}");
                return StatusCode(500, $"Error interno del servidor: {ex.Message}");
            }
            finally
            {
                controlConexion.CerrarBd();
            }
        }

        //agregado por mi
        [AllowAnonymous]
        [HttpGet("join")]
        public IActionResult GetJoinedData(
            string projectName, string tableName,
            [FromQuery] List<string> joinTables,
            [FromQuery] List<string> onConditions,
            [FromQuery] List<string> selectedColumns
        ) // Lista de columnas específicas
        {
            if (string.IsNullOrWhiteSpace(tableName) || joinTables == null || joinTables.Count == 0 || onConditions == null || onConditions.Count == 0 || selectedColumns == null || selectedColumns.Count == 0)
            {
                return BadRequest("El nombre de la tabla principal, las tablas de unión, las condiciones ON y las columnas de selección no pueden estar vacíos.");
            }

            if (joinTables.Count != onConditions.Count)
            {
                return BadRequest("El número de tablas de unión y condiciones ON debe ser igual.");
            }

            controlConexion.AbrirBd();
            try
            {
                // Construcción del SELECT
                var selectClause = string.Join(", ", selectedColumns);

                // Construcción de los JOINs
                var joins = new StringBuilder();
                for (int i = 0; i < joinTables.Count; i++)
                {
                    joins.Append($" INNER JOIN {joinTables[i]} ON {onConditions[i]}");
                }

                string query = $"SELECT {selectClause} FROM {tableName}{joins}";
                Console.WriteLine($"Executing SQL query: {query}");

                var resultado = controlConexion.EjecutarConsultaSql(query, Array.Empty<DbParameter>());

                var filas = resultado.AsEnumerable()
                                        .Select(row => resultado.Columns.Cast<DataColumn>()
                                                    .ToDictionary(col => col.ColumnName, col => row[col] == DBNull.Value ? null : row[col]))
                                        .ToList();

                return Ok(filas); // Retorna el resultado en formato JSON.

            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occurred: {ex.Message}");
                return StatusCode(500, $"Error interno del servidor: {ex.Message}");
            }
            finally
            {
                controlConexion.CerrarBd();
            }
        }

        [AllowAnonymous]
        [HttpGet("getPk")]
        public IActionResult GetPrimaryKey(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                // Devolver un error en formato JSON
                return BadRequest(new { Error = "El nombre de la tabla no puede estar vacío." });
            }

            controlConexion.AbrirBd();
            try
            {
                // Consulta para obtener la clave primaria y su tipo de dato de una tabla específica
                string query = @"
            SELECT 
                COLUMN_NAME, 
                DATA_TYPE 
            FROM 
                INFORMATION_SCHEMA.COLUMNS 
            WHERE 
                COLUMN_NAME IN (
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
                    AND TABLE_NAME = @tableName
                )
                AND TABLE_NAME = @tableName";

                var parameters = new DbParameter[]
                {
            CreateParameter("@tableName", tableName)
                };

                var resultado = controlConexion.EjecutarConsultaSql(query, parameters);

                if (resultado == null || resultado.Rows.Count == 0)
                {
                    // Si no se encuentra la clave primaria, retornar un error en formato JSON
                    return NotFound(new { Error = "No se encontró la clave primaria para la tabla especificada." });
                }

                // Convertir el resultado a un formato JSON usando la lógica mencionada
                var filas = resultado.AsEnumerable()
                                     .Select(row => resultado.Columns.Cast<DataColumn>()
                                                 .ToDictionary(col => col.ColumnName, col => row[col] == DBNull.Value ? null : row[col]))
                                     .ToList();

                // Retorna el resultado en formato JSON
                return Ok(filas);
            }
            catch (Exception ex)
            {
                // Capturar errores y devolver un mensaje en formato JSON
                Console.WriteLine($"Exception occurred: {ex.Message}");
                return StatusCode(500, new { Error = $"Error interno del servidor: {ex.Message}" });
            }
            finally
            {
                controlConexion.CerrarBd();
            }
        }

        // Método privado para convertir un JsonElement en su tipo correspondiente.
        private object? ConvertJsonElement(JsonElement jsonElement)
        {
            if (jsonElement.ValueKind == JsonValueKind.Null)
                return null;

            switch (jsonElement.ValueKind)
            {
                case JsonValueKind.String:
                    return DateTime.TryParse(jsonElement.GetString(), out DateTime dateValue) ? (object)dateValue : jsonElement.GetString();
                case JsonValueKind.Number:
                    return jsonElement.TryGetInt32(out var intValue) ? (object)intValue : jsonElement.GetDouble();
                case JsonValueKind.True:
                    return true;
                case JsonValueKind.False:
                    return false;
                case JsonValueKind.Null:
                    return null;
                case JsonValueKind.Object:
                    return jsonElement.GetRawText();
                case JsonValueKind.Array:
                    return jsonElement.GetRawText();
                default:
                    throw new InvalidOperationException($"Unsupported JsonValueKind: {jsonElement.ValueKind}");
            }
        }

        [AllowAnonymous] // Permite el acceso anónimo a este método.
        [HttpPost] // Define una ruta HTTP POST para este método.
        public IActionResult Crear(string projectName, string tableName, [FromBody] Dictionary<string, object?> entidadData)  // Crea una nueva fila en la tabla especificada.
        {
            if (string.IsNullOrWhiteSpace(tableName) || entidadData == null || !entidadData.Any())  // Verifica si el nombre de la tabla o los datos están vacíos.
                return BadRequest("El nombre de la tabla y los datos de la entidad no pueden estar vacíos.");  // Retorna un error si algún parámetro está vacío.

            try
            {
                var propiedades = entidadData.ToDictionary(  // Convierte los datos de la entidad en un diccionario de propiedades.
                    kvp => kvp.Key,
                    kvp => kvp.Value is JsonElement jsonElement ? ConvertJsonElement(jsonElement) : kvp.Value);

                // Verifica si hay un campo de contraseña en los datos, y si lo hay, lo hashea.
                var passwordKeys = new[] { "password", "contrasena", "passw" };  // Lista de posibles nombres para campos de contraseña.
                var passwordKey = propiedades.Keys.FirstOrDefault(k => passwordKeys.Any(pk => k.IndexOf(pk, StringComparison.OrdinalIgnoreCase) >= 0));  // Busca si alguno de los campos es una contraseña.
                
                if (passwordKey != null)  // Si se encontró un campo de contraseña.
                {
                    var plainPassword = propiedades[passwordKey]?.ToString();  // Obtiene el valor de la contraseña.
                    if (!string.IsNullOrEmpty(plainPassword))  // Si la contraseña no está vacía.
                    {
                        propiedades[passwordKey] = BCrypt.Net.BCrypt.HashPassword(plainPassword);  // Hashea la contraseña.
                    }
                }

                string provider = _configuration["DatabaseProvider"] ?? throw new InvalidOperationException("DatabaseProvider not configured.");  // Obtiene el proveedor de base de datos.
                var columnas = string.Join(",", propiedades.Keys);  // Une los nombres de las columnas en una cadena.
                var valores = string.Join(",", propiedades.Keys.Select(k => $"{GetParameterPrefix(provider)}{k}"));  // Une los nombres de los valores en una cadena con su prefijo.
                string comandoSQL = $"INSERT INTO {tableName} ({columnas}) VALUES ({valores})";  // Crea la consulta SQL para insertar una nueva fila.

                var parametros = propiedades.Select(p => CreateParameter($"{GetParameterPrefix(provider)}{p.Key}", p.Value)).ToArray();  // Crea los parámetros para la consulta SQL.

                Console.WriteLine($"Executing SQL query: {comandoSQL} with parameters:");  // Muestra la consulta SQL y los parámetros en la consola.
                foreach (var parametro in parametros)  // Recorre cada parámetro.
                {
                    Console.WriteLine($"{parametro.ParameterName} = {parametro.Value}, DbType: {parametro.DbType}");  // Muestra el nombre y valor del parámetro en la consola.
                }

                controlConexion.AbrirBd();  // Abre la conexión a la base de datos.
                controlConexion.EjecutarComandoSql(comandoSQL, parametros);  // Ejecuta la consulta SQL para insertar la nueva fila.
                controlConexion.CerrarBd();  // Cierra la conexión a la base de datos.

                return Ok("Entidad creada exitosamente.");  // Retorna una respuesta de éxito.
            }
            catch (Exception ex)  // Captura cualquier excepción que ocurra durante la ejecución.
            {
                Console.WriteLine($"Exception occurred: {ex.Message}");  // Muestra el mensaje de la excepción en la consola.
                return StatusCode(500, $"Error interno del servidor: {ex.Message}");  // Retorna un error 500 si ocurre una excepción.
            }
        }

        [AllowAnonymous] // Permite el acceso anónimo a este método.
        [HttpPut("{keyName}/{keyValue}")] // Define una ruta HTTP PUT con parámetros adicionales.
        public IActionResult Actualizar(string projectName, string tableName, string keyName, string keyValue, [FromBody] Dictionary<string, object?> entidadData) // Actualiza una fila en la tabla basada en una clave.
        {
            if (string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(keyName) || entidadData == null || !entidadData.Any()) // Verifica si alguno de los parámetros está vacío.
                return BadRequest("El nombre de la tabla, el nombre de la clave y los datos de la entidad no pueden estar vacíos."); // Retorna un error si algún parámetro está vacío.

            try
            {
                var propiedades = entidadData.ToDictionary( // Convierte los datos de la entidad en un diccionario de propiedades.
                    kvp => kvp.Key,
                    kvp => kvp.Value is JsonElement jsonElement ? ConvertJsonElement(jsonElement) : kvp.Value);

                // Verifica si hay un campo de contraseña en los datos, y si lo hay, lo hashea.
                var passwordKeys = new[] { "password", "contrasena", "passw" }; // Lista de posibles nombres para campos de contraseña.
                var passwordKey = propiedades.Keys.FirstOrDefault(k => passwordKeys.Any(pk => k.IndexOf(pk, StringComparison.OrdinalIgnoreCase) >= 0)); // Busca si alguno de los campos es una contraseña.
                
                if (passwordKey != null) // Si se encontró un campo de contraseña.
                {
                    var plainPassword = propiedades[passwordKey]?.ToString(); // Obtiene el valor de la contraseña.
                    if (!string.IsNullOrEmpty(plainPassword)) // Si la contraseña no está vacía.
                    {
                        propiedades[passwordKey] = BCrypt.Net.BCrypt.HashPassword(plainPassword); // Hashea la contraseña.
                    }
                }

                string provider = _configuration["DatabaseProvider"] ?? throw new InvalidOperationException("DatabaseProvider not configured."); // Obtiene el proveedor de base de datos.
                var actualizaciones = string.Join(",", propiedades.Select(p => $"{p.Key}={GetParameterPrefix(provider)}{p.Key}")); // Crea la cadena de actualizaciones para la consulta SQL.
                string comandoSQL = $"UPDATE {tableName} SET {actualizaciones} WHERE {keyName}={GetParameterPrefix(provider)}KeyValue"; // Crea la consulta SQL para actualizar la fila.

                var parametros = propiedades.Select(p => CreateParameter($"{GetParameterPrefix(provider)}{p.Key}", p.Value)).ToList(); // Crea los parámetros para la consulta SQL.
                parametros.Add(CreateParameter($"{GetParameterPrefix(provider)}KeyValue", keyValue)); // Agrega el parámetro para la clave de la fila a actualizar.

                Console.WriteLine($"Executing SQL query: {comandoSQL} with parameters:"); // Muestra la consulta SQL y los parámetros en la consola.
                foreach (var parametro in parametros) // Recorre cada parámetro.
                {
                    Console.WriteLine($"{parametro.ParameterName} = {parametro.Value}, DbType: {parametro.DbType}"); // Muestra el nombre y valor del parámetro en la consola.
                }

                controlConexion.AbrirBd(); // Abre la conexión a la base de datos.
                controlConexion.EjecutarComandoSql(comandoSQL, parametros.ToArray()); // Ejecuta la consulta SQL para actualizar la fila.
                controlConexion.CerrarBd(); // Cierra la conexión a la base de datos.

                return Ok("Entidad actualizada exitosamente."); // Retorna una respuesta de éxito.
            }
            catch (Exception ex) // Captura cualquier excepción que ocurra durante la ejecución.
            {
                Console.WriteLine($"Exception occurred: {ex.Message}"); // Muestra el mensaje de la excepción en la consola.
                return StatusCode(500, $"Error interno del servidor: {ex.Message}"); // Retorna un error 500 si ocurre una excepción.
            }
        }

        // Método privado para obtener el prefijo adecuado para los parámetros SQL, según el proveedor de la base de datos.
        private string GetParameterPrefix(string provider)
        {
            return "@"; // Para SQL Server y LocalDB, el prefijo es "@".
        }

        [AllowAnonymous] // Permite el acceso anónimo a este método.
        [HttpDelete("{keyName}/{keyValue}")] // Define una ruta HTTP DELETE con parámetros adicionales.
        public IActionResult Eliminar(string projectName, string tableName, string keyName, string keyValue) // Elimina una fila de la tabla basada en una clave.
        {
            if (string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(keyName)) // Verifica si alguno de los parámetros está vacío.
                return BadRequest("El nombre de la tabla o el nombre de la clave no pueden estar vacíos."); // Retorna un error si algún parámetro está vacío.

            try
            {
                string provider = _configuration["DatabaseProvider"] ?? throw new InvalidOperationException("DatabaseProvider not configured."); // Obtiene el proveedor de base de datos.
                string comandoSQL = $"DELETE FROM {tableName} WHERE {keyName}=@KeyValue"; // Crea la consulta SQL para eliminar la fila.
                var parametro = CreateParameter("@KeyValue", keyValue); // Crea el parámetro para la clave de la fila a eliminar.

                controlConexion.AbrirBd(); // Abre la conexión a la base de datos.
                controlConexion.EjecutarComandoSql(comandoSQL, new[] { parametro }); // Ejecuta la consulta SQL para eliminar la fila.
                controlConexion.CerrarBd(); // Cierra la conexión a la base de datos.

                return Ok("Entidad eliminada exitosamente."); // Retorna una respuesta de éxito.
            }
            catch (Exception ex) // Captura cualquier excepción que ocurra durante la ejecución.
            {
                return StatusCode(500, $"Error interno del servidor: {ex.Message}"); // Retorna un error 500 si ocurre una excepción.
            }
        }

        [AllowAnonymous] // Permite el acceso anónimo a este método.
        [HttpGet("/")] // Define una ruta HTTP GET en la raíz de la API.
        public IActionResult GetRoot() // Método que retorna un mensaje indicando que la API está en funcionamiento.
        {
            return Ok("API is running"); // Retorna un mensaje indicando que la API está en funcionamiento.
        }

        [AllowAnonymous] // Permite el acceso anónimo a este método.
        [HttpPost("verificar-contrasena")] // Define una ruta HTTP POST para verificar contraseñas.
        public IActionResult VerificarContrasena(string projectName, string tableName, [FromBody] Dictionary<string, string> datos) // Verifica si la contraseña proporcionada coincide con la almacenada.
        {
            if (string.IsNullOrWhiteSpace(tableName) || datos == null || !datos.ContainsKey("campoUsuario") || !datos.ContainsKey("campoContrasena") || !datos.ContainsKey("valorUsuario") || !datos.ContainsKey("valorContrasena")) // Verifica si alguno de los parámetros está vacío.
                return BadRequest("El nombre de la tabla, el campo de usuario, el campo de contraseña, el valor de usuario y el valor de contraseña no pueden estar vacíos."); // Retorna un error si algún parámetro está vacío.

            try
            {
                string campoUsuario = datos["campoUsuario"]; // Obtiene el nombre del campo de usuario.
                string campoContrasena = datos["campoContrasena"]; // Obtiene el nombre del campo de contraseña.
                string valorUsuario = datos["valorUsuario"]; // Obtiene el valor del usuario.
                string valorContrasena = datos["valorContrasena"]; // Obtiene el valor de la contraseña.

                string proveedor = _configuration["DatabaseProvider"] ?? throw new InvalidOperationException("Proveedor de base de datos no configurado."); // Obtiene el proveedor de base de datos.
                string consultaSQL = $"SELECT {campoContrasena} FROM {tableName} WHERE {campoUsuario} = @ValorUsuario"; // Crea la consulta SQL para obtener la contraseña almacenada.
                var parametro = CreateParameter("@ValorUsuario", valorUsuario); // Crea el parámetro para el valor del usuario.

                controlConexion.AbrirBd(); // Abre la conexión a la base de datos.
                var resultado = controlConexion.EjecutarConsultaSql(consultaSQL, new DbParameter[] { parametro }); // Ejecuta la consulta SQL para obtener la contraseña.
                controlConexion.CerrarBd(); // Cierra la conexión a la base de datos.

                if (resultado.Rows.Count == 0) // Verifica si no se encontró el usuario.
                {
                    return NotFound("Usuario no encontrado."); // Retorna un error 404 si no se encontró el usuario.
                }

                string contrasenaHasheada = resultado.Rows[0][campoContrasena]?.ToString() ?? string.Empty; // Obtiene la contraseña hasheada almacenada.

                // Verifica si el hash de la contraseña es válido.
                if (!contrasenaHasheada.StartsWith("$2"))
                {
                    throw new InvalidOperationException("El hash de la contraseña almacenada no es un hash válido de BCrypt."); // Lanza una excepción si el hash almacenado no es válido.
                }

                bool esContrasenaValida = BCrypt.Net.BCrypt.Verify(valorContrasena, contrasenaHasheada); // Verifica si la contraseña proporcionada coincide con el hash almacenado.

                if (esContrasenaValida) // Si la contraseña es válida.
                {
                    Console.WriteLine("Contraseña verificada exitosamente."); // Muestra un mensaje en la consola.
                    return Ok("Contraseña verificada exitosamente."); // Retorna una respuesta de éxito.
                }
                else // Si la contraseña no es válida.
                {
                    return Unauthorized("Contraseña incorrecta."); // Retorna un error 401 si la contraseña es incorrecta.
                }
            }
            catch (Exception ex) // Captura cualquier excepción que ocurra durante la ejecución.
            {
                Console.WriteLine($"Ocurrió una excepción: {ex.Message}"); // Muestra el mensaje de la excepción en la consola.
                return StatusCode(500, $"Error interno del servidor: {ex.Message}"); // Retorna un error 500 si ocurre una excepción.
            }
        }
        // Método para crear un parámetro de consulta SQL basado en el proveedor de base de datos.
        public DbParameter CreateParameter(string name, object? value)
        {
            return new SqlParameter(name, value ?? DBNull.Value); // Crea un parámetro para SQL Server y LocalDB.
        }

        [AllowAnonymous]
        [HttpPost("ejecutar-consulta-parametrizada")]
        public IActionResult EjecutarConsultaParametrizada([FromBody] JsonElement cuerpoSolicitud)
        {
            try
            {
                // Verifica si el cuerpo de la solicitud contiene la consulta SQL
                if (!cuerpoSolicitud.TryGetProperty("consulta", out var consultaElement) || consultaElement.ValueKind != JsonValueKind.String)
                {
                    return BadRequest("Debe proporcionar una consulta SQL válida en el cuerpo de la solicitud.");
                }

                string consultaSQL = consultaElement.GetString() ?? throw new ArgumentException("La consulta SQL no puede estar vacía.");

                // Verifica si el cuerpo de la solicitud contiene los parámetros
                var parametros = new List<DbParameter>();
                if (cuerpoSolicitud.TryGetProperty("parametros", out var parametrosElement) && parametrosElement.ValueKind == JsonValueKind.Object)
                {
                    foreach (var parametro in parametrosElement.EnumerateObject())
                    {
                        string paramName = parametro.Name.StartsWith("@") ? parametro.Name : "@" + parametro.Name;
                        object? paramValue = parametro.Value.ValueKind == JsonValueKind.Null ? DBNull.Value : parametro.Value.GetRawText().Trim('"');
                        parametros.Add(controlConexion.CreateParameter(paramName, paramValue));
                    }
                }

                // Abrir la conexión a la base de datos
                controlConexion.AbrirBd();

                // Ejecutar la consulta SQL
                var resultado = controlConexion.EjecutarConsultaSql(consultaSQL, parametros.ToArray());

                // Cerrar la conexión a la base de datos
                controlConexion.CerrarBd();

                // Verifica si hay resultados
                if (resultado.Rows.Count == 0)
                {
                    return NotFound("No se encontraron resultados para la consulta proporcionada.");
                }

                // Procesar resultados a formato JSON
                var lista = new List<Dictionary<string, object?>>();
                foreach (DataRow fila in resultado.Rows)
                {
                    var propiedades = resultado.Columns.Cast<DataColumn>()
                        .ToDictionary(col => col.ColumnName, col => fila[col] == DBNull.Value ? null : fila[col]);
                    lista.Add(propiedades);
                }

                // Retornar resultados en formato JSON
                return Ok(lista);
            }
            catch (SqlException sqlEx)
            {
                // Manejo de excepciones SQL
                controlConexion.CerrarBd(); // Asegura que la conexión se cierre en caso de error
                Console.WriteLine($"SQL Error: {sqlEx.Message}");
                return StatusCode(500, new { Mensaje = "Error en la base de datos.", Detalle = sqlEx.Message });
            }
            catch (Exception ex)
            {
                // Manejo de excepciones generales
                controlConexion.CerrarBd(); // Asegura que la conexión se cierre en caso de error
                Console.WriteLine($"Error: {ex.Message}");
                return StatusCode(500, new { Mensaje = "Se presentó un error:", Detalle = ex.Message });
            }
        }

        [AllowAnonymous]
        [HttpPost("ejecutar-procedimiento/{procedureName}")]
        public IActionResult EjecutarProcedimientoAlmacenado(string procedureName, [FromBody] JsonElement body)
        {
            // Verificar que el nombre del procedimiento no esté vacío
            if (string.IsNullOrWhiteSpace(procedureName))
            {
                return BadRequest(new { Mensaje = "El nombre del procedimiento es requerido." });
            }

            try
            {
                // Abrir la conexión a la base de datos
                controlConexion.AbrirBd();

                // Obtener la conexión
                var connection = controlConexion.GetConnection();
                if (connection == null || connection.State != ConnectionState.Open)
                {
                    return StatusCode(500, "No se pudo obtener una conexión válida a la base de datos.");
                }

                using (var command = new SqlCommand(procedureName, (SqlConnection)connection))
                {
                    command.CommandType = CommandType.StoredProcedure;

                    // Agregar parámetros al comando
                    foreach (var property in body.EnumerateObject())
                    {
                        string paramName = property.Name.StartsWith("@") ? property.Name : "@" + property.Name;
                        if (property.Name.EndsWith("productos") && property.Value.ValueKind == JsonValueKind.Array)
                        {
                            var productosJson = JsonSerializer.Serialize(property.Value);
                            command.Parameters.AddWithValue(paramName, productosJson);
                        }
                        else
                        {
                            command.Parameters.AddWithValue(paramName, property.Value.GetRawText().Trim('"'));
                        }
                    }

                    // Ejecutar el procedimiento almacenado
                    int filasAfectadas = command.ExecuteNonQuery();
                    controlConexion.CerrarBd(); // Cerrar la conexión a la base de datos

                    return Ok(new { Mensaje = "Procedimiento almacenado ejecutado exitosamente.", FilasAfectadas = filasAfectadas });
                }
            }
            catch (SqlException sqlEx)
            {
                controlConexion.CerrarBd(); // Asegura cerrar la conexión en caso de error
                Console.WriteLine($"SQL Error: {sqlEx.Message}");
                return StatusCode(500, new { Mensaje = "Error en la base de datos.", Detalle = sqlEx.Message });
            }
            catch (Exception ex)
            {
                controlConexion.CerrarBd(); // Asegura cerrar la conexión en caso de error general
                Console.WriteLine($"Error: {ex.Message}");
                return StatusCode(500, new { Mensaje = "Se presentó Un error:", Detalle = ex.Message });
            }
        }

    }
}
