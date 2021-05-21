using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using MySql.Data.MySqlClient;

namespace Mysql多行数据插入案例
{
	public class Program
	{
		  static async Task Main(string[] args)
		{

			var test = new Test();
			//单条
			// await test.TestFuc();

			//合并插入
			// await test.TestMerge();

			//文件读取
			await test.TestBulk();
		}
	}

	public class Test:DataBaseConfig
	{
		//单条数据插入
		public async Task TestFuc()
		{
			//开始时间
			var startTime = DateTime.Now;
			Console.WriteLine("10w单条数据插入开始：");
			try
			{
				await using var conn = GetMySqlConnection();
				if (conn.State == ConnectionState.Closed)
				{
					await conn.OpenAsync();
				}
				//插入10万数据
				for (var i = 0; i < 100000; i++)
				{
					//插入
					var sql = string.Format("insert into trade(id,trade_no) values('{0}','{1}');",
						Guid.NewGuid().ToString(), "trade_" + (i + 1)
					);

					await conn.ExecuteAsync(sql);
				}
				
			}
			catch (Exception ex)
			{
				throw;
			}
			//完成时间
			var endTime = DateTime.Now;
			//耗时
			var spanTime = endTime - startTime;
			Console.WriteLine("10w条数据循环插入结束，方式耗时：" + spanTime.Minutes + "分" + spanTime.Seconds + "秒" + spanTime.Milliseconds + "毫秒");
		}

		//合并
		public async Task TestMerge()
		{
			//开始时间
			var startTime = DateTime.Now;
			Console.WriteLine("10w条数据合并插入开始：");
			try
			{
				await using var conn = GetMySqlConnection();
				if (conn.State == ConnectionState.Closed)
				{
					await conn.OpenAsync();
				}
				//插入10万数据
				var sql = new StringBuilder();
				for (var i = 0; i < 100000; i++)
				{
					if (i % 1000 ==  0)
					{
						sql.Append("insert into trade(id,trade_no) values ");
					}
					//拼接
					sql.AppendFormat("('{0}','{1}'),", Guid.NewGuid().ToString(), "trade_" + (i + 1));


					//一次性插入1000条
					if (i % 1000 == 999)
					{
						await conn.ExecuteAsync(sql.ToString().TrimEnd(','));
					}
				}
			}
			catch (Exception ex)
			{
				throw;
			}
			//完成时间
			var endTime = DateTime.Now;
			//耗时
			var spanTime = endTime - startTime;
			Console.WriteLine("10w条数据合并循环插入结束，方式耗时：" + spanTime.Minutes + "分" + spanTime.Seconds + "秒" + spanTime.Milliseconds + "毫秒");
		}


		/// <summary>
		/// MySqlBulkLoader 文件读取数据
		/// 要开启数据库 secure_file_priv 允许上传文件 修改mysql.ini文件
		/// </summary>
		/// <returns></returns>
		public async Task TestBulk()
		{
			//开始时间
			var startTime = DateTime.Now;
			Console.WriteLine("10w条数据插入开始：");
			try
			{
				await using var conn = GetMySqlConnection();
				if (conn.State == ConnectionState.Closed)
				{
					await conn.OpenAsync();
				}
				var table = new DataTable();
				table.Columns.Add("id", typeof(string));
				table.Columns.Add("trade_no", typeof(string));

				//生成10万数据
				for (var i = 0; i < 100000; i++)
				{
					if (i % 500000 == 0)
					{
						table.Rows.Clear();
					}

					//记录
					var row = table.NewRow();
					row[0] = Guid.NewGuid().ToString();
					row[1] = "trade_" + (i + 1);
					table.Rows.Add(row);

					//50万条一批次插入
					if (i % 500000 != 499999 && i < (100000 - 1))
					{
						continue;
					}

					Console.WriteLine("开始插入:" + i);

					//数据转换为csv格式
					var tradeCsv = DataTableToCsv(table);
					var tradeFilePath = System.AppDomain.CurrentDomain.BaseDirectory + "trade.csv";
					File.WriteAllText(tradeFilePath, tradeCsv);
					#region 保存至数据库
					var bulkCopy = new MySqlBulkLoader(conn)
					{
						FieldTerminator = ",",
						FieldQuotationCharacter = '"',
						EscapeCharacter = '"',
						LineTerminator = "\r\n",
						FileName = tradeFilePath,
						NumberOfLinesToSkip = 0,
						TableName = "trade"
					};
					bulkCopy.Columns.AddRange(table.Columns.Cast<DataColumn>().Select(colum => colum.ColumnName).ToList());
					bulkCopy.Load();

					#endregion
				}

			}
			catch (Exception ex)
			{
				throw;
			}
			//完成时间
			var endTime = DateTime.Now;
			//耗时
			var spanTime = endTime - startTime;
			Console.WriteLine("10w条数据插入结束，方式耗时：" + spanTime.Minutes + "分" + spanTime.Seconds + "秒" + spanTime.Milliseconds + "毫秒");
		}

		///将DataTable转换为标准的CSV  
		/// </summary>  
		/// <param name="table">数据表</param>  
		/// <returns>返回标准的CSV</returns>  
		private static string DataTableToCsv(DataTable table)
		{
			//以半角逗号（即,）作分隔符，列为空也要表达其存在。  
			//列内容如存在半角逗号（即,）则用半角引号（即""）将该字段值包含起来。  
			//列内容如存在半角引号（即"）则应替换成半角双引号（""）转义，并用半角引号（即""）将该字段值包含起来。  
			StringBuilder sb = new StringBuilder();
			DataColumn colum;
			foreach (DataRow row in table.Rows)
			{
				for (int i = 0; i < table.Columns.Count; i++)
				{
					colum = table.Columns[i];
					if (i != 0) sb.Append(",");
					if (colum.DataType == typeof(string) && row[colum].ToString().Contains(","))
					{
						sb.Append("\"" + row[colum].ToString().Replace("\"", "\"\"") + "\"");
					}
					else sb.Append(row[colum].ToString());
				}
				sb.AppendLine();
			}
			return sb.ToString();
		}
	}

	
}
