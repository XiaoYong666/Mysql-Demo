using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Text;
using MySql.Data.MySqlClient;

namespace Mysql多行数据插入案例
{
	public class DataBaseConfig : IDisposable
	{
		public static IConfigurationRoot Configuration { get; set; }

		private MySqlConnection conn;

		public MySqlConnection GetMySqlConnection(int databaseId = 0, bool open = true,
			bool convertZeroDatetime = false, bool allowZeroDatetime = false)
		{
			var builder = new ConfigurationBuilder()
				.SetBasePath(Directory.GetCurrentDirectory());
				// .AddJsonFile("appsettings.json");

			Configuration = builder.Build();

			string cs =
				"server=localhost;port=3306;uid=root;pwd=xiaoyong;database=testMoreData;SslMode=None;Allow User Variables=True";//Configuration["DBConnection:MySqlConnectionString"];
			var csb = new MySqlConnectionStringBuilder(cs)
			{
				AllowZeroDateTime = allowZeroDatetime,
				ConvertZeroDateTime = convertZeroDatetime
			};

			conn = new MySqlConnection(csb.ConnectionString);
			return conn;
		}
		public void Dispose()
		{
			if (conn != null && conn.State != System.Data.ConnectionState.Closed)
			{
				conn.Close();
			}
		}
	}
}
