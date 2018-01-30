using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DocDbClient
{
    static public class Logger
    {
        private static ConsoleColor _originalColor = Console.ForegroundColor;
        public static void Info(string message)
        {
            //Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine($"[{GetDateTimeMarker()}] {message}");
            //Console.ForegroundColor = _originalColor;
        }

        public static void Error(string message)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"[{GetDateTimeMarker()}] {message}");
            Console.ForegroundColor = _originalColor;

        }

        private static string GetDateTimeMarker()
        {
            return DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss:ffffff");
        }
    }
}
