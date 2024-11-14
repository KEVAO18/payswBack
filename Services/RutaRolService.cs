using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace ProyectoBackendCsharp.Services
{
    public class RutaRolService
    {
        private readonly ControlConexion controlConexion; // Servicio para el manejo de la base de datos.
        private readonly IConfiguration _configuration;   // Configuración de la aplicación.

        public RutaRolService(ControlConexion controlConexion, IConfiguration configuration)
        {
            this.controlConexion = controlConexion ?? throw new ArgumentNullException(nameof(controlConexion));
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        // Método para verificar si un usuario tiene acceso a una ruta específica
        public bool VerificarAcceso(string ruta, string rolUsuario)
        {
            try
            {
                // Define la consulta SQL para verificar el acceso del usuario a la ruta
                string consultaSQL = "SELECT COUNT(*) FROM RutaRol WHERE Ruta = @Ruta AND Rol = @Rol";
                var parametros = new[]
                {
            controlConexion.CreateParameter("@Ruta", ruta),
            controlConexion.CreateParameter("@Rol", rolUsuario)
        };

                controlConexion.AbrirBd(); // Abre la conexión a la base de datos
                var resultado = controlConexion.EjecutarConsultaSql(consultaSQL, parametros); // Ejecuta la consulta
                controlConexion.CerrarBd(); // Cierra la conexión a la base de datos

                if (resultado.Rows.Count > 0 && Convert.ToInt32(resultado.Rows[0][0]) > 0)
                {
                    return true; // El usuario tiene acceso a la ruta
                }

                return false; // El usuario no tiene acceso a la ruta
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al verificar acceso: {ex.Message}");
                controlConexion.CerrarBd(); // Asegura cerrar la conexión en caso de error
                throw;
            }
        }


    }
}
