using System;
using System.Data.Common;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Cat.Database
{
public enum DbPagerKind
{
    Row_Number = 1,

    TableVar = 2,

    MaxMin = 3,

    TopTop = 4,

    Max_TopTop = 304,

    Max_TableVar = 302
}

public class DbPagerBase
{
    public DbPager dbPager;

    internal virtual void CreateRecordCountSQL()
    {
        if (dbPager.isDefaultRecordCount)
        {
            if (dbPager.kind == DbPagerKind.Row_Number)
            {
                dbPager.pagerCountSql = String.Format("SELECT COUNT(0) FROM {0} {1}", dbPager.tableName, dbPager.appendSql);
            }
            else
            {
                dbPager.pagerCountSql = String.Format("SELECT COUNT(0) FROM {0} {1}", dbPager.tableName, string.IsNullOrEmpty(dbPager.whereSql) ? "" : "WHERE " + dbPager.whereSql);
            }
        }
        else
        {
            if (dbPager.kind == DbPagerKind.Row_Number)
            {
                dbPager.pagerCountSql = String.Format("SELECT COUNT(0) FROM (SELECT {0} FROM {1} {2}) AS [TABLE]", dbPager.field, dbPager.tableName, dbPager.appendSql);
            }
            else
            {
                dbPager.pagerCountSql = String.Format("SELECT COUNT(0) FROM (SELECT {0} FROM {1} {2}) AS [TABLE]", dbPager.field, dbPager.tableName, string.IsNullOrEmpty(dbPager.whereSql) ? "" : "WHERE " + dbPager.whereSql);
            }
        }
    }

    internal virtual void CreateFirstPageSQL()
    {
        StringBuilder sql = new StringBuilder();
        sql.Append("select top ");
        sql.Append(dbPager.pagerSize);
        sql.Append(" ");
        sql.Append(dbPager.field);
        sql.Append(" from ");
        sql.Append(dbPager.tableName);
        if (dbPager.kind == DbPagerKind.Row_Number || dbPager.kind == DbPagerKind.TableVar)
        {
            if (!string.IsNullOrEmpty(dbPager.appendSql))
            {
                sql.Append(" ");
                sql.Append(dbPager.appendSql);
            }
        }
        else
        {
            if (!string.IsNullOrEmpty(dbPager.whereSql))
            {
                sql.Append(" where ");
                sql.Append(dbPager.whereSql);
            }
        }
        sql.Append(" order by ");
        sql.Append(dbPager.orderSql);

        dbPager.firstPagerSql = sql.ToString();
    }

    internal virtual void CreateNextPageSQL()
    {

    }

    internal virtual void CreateLastPageSQL()
    {

    }

    internal virtual string GetSQLByPageIndex()
    {

        if (dbPager.pagerIndex == 1)
        {
            return dbPager.firstPagerSql;
        }

        if (dbPager.pagerIndex < 1)
            dbPager.pagerIndex = 1;

        if (dbPager.pagerIndex > dbPager.pagerCount)
            dbPager.pagerIndex = dbPager.pagerCount;


        Int32 p1 = dbPager.pagerSize * (dbPager.pagerIndex - 1) + 1;
        Int32 p2 = p1 + dbPager.pagerSize - 1;

        return string.Format(dbPager.pagerSql, p1, p2);

    }
}

public class SQL_Max : DbPagerBase
{
    internal override void CreateNextPageSQL()
    {



        /*

        select top 10 * from table
        where customerID >=
            (SELECT max(customerID) FROM
                (select top 21 customerID from table order by customerID   ) as t
            )

        ---------------------

        select top 10 * from table where customerID  >=(
            select top 1 customerID from (
                SELECT  top 21 customerID
                    FROM table  order by customerID ) as aa   order by customerID desc
                )


        ------------
        declare @id int

        SELECT top 21 @id = customerID
        FROM table

        select top 10 * from table where customerID  >=@id
        select @id

         */

        string orderCol;                   bool isDesc = true;
        orderCol = dbPager.orderSql.ToLower();              if (orderCol.Contains("desc"))
        {
            orderCol = orderCol.Replace("desc", "");
        }
        else
        {
            orderCol = orderCol.Replace("asc", "");
            isDesc = false;
        }

        StringBuilder sql = new StringBuilder();
        sql.Append(" select top ");            sql.Append(dbPager.pagerSize);                sql.Append(" ");
        sql.Append(dbPager.field);
        sql.Append(" from ");
        sql.Append(dbPager.tableName);

        sql.Append(" where ");                  sql.Append(orderCol);

        sql.Append(isDesc ? "<=" : ">=");
        sql.Append(" (SELECT ");               sql.Append(isDesc ? "min(" : "max(");

        sql.Append(orderCol);
        sql.Append(" ) from (select top {0} ");                 sql.Append(orderCol);
        sql.Append(" from ");
        sql.Append(dbPager.tableName);

        if (!string.IsNullOrEmpty(dbPager.whereSql))               {
            sql.Append(" where ");
            sql.Append(dbPager.whereSql);
        }

        sql.Append(" order by ");                   sql.Append(orderCol);

        if (isDesc)
            sql.Append(" desc");


        sql.Append(" ) as t ) ");
        if (!string.IsNullOrEmpty(dbPager.whereSql))               {
            sql.Append(" and ( ");
            sql.Append(dbPager.whereSql);
            sql.Append(")");
        }

        sql.Append(" order by ");                  sql.Append(orderCol);

        if (isDesc)
            sql.Append(" desc ");


        dbPager.pagerSql = sql.ToString();
    }

    internal override string GetSQLByPageIndex()
    {
        if (dbPager.pagerIndex == 1)
        {
            return dbPager.firstPagerSql;
        }

        if (dbPager.pagerIndex < 1)
            dbPager.pagerIndex = 1;

        if (dbPager.pagerIndex > dbPager.pagerCount)
            dbPager.pagerIndex = dbPager.pagerCount;


        Int32 p1 = dbPager.pagerSize * (dbPager.pagerIndex - 1) + 1;

        return string.Format(dbPager.pagerSql, p1);

    }
}

public class SQL_Row_Number : DbPagerBase
{
    internal override void CreateNextPageSQL()
    {
        StringBuilder sql = new StringBuilder();
        sql.Append("with t_pager as (select row_num = ROW_NUMBER() OVER (ORDER BY ");
        sql.Append(dbPager.orderSql);
        sql.Append(" ),");
        sql.Append(dbPager.field);
        sql.Append(" from ");
        sql.Append(dbPager.tableName);

        if (!string.IsNullOrEmpty(dbPager.appendSql))
        {
            sql.Append(" ");
            sql.Append(dbPager.appendSql);
        }

        sql.Append(" ) select * from t_pager where row_num between {0} and {1} ");


        dbPager.pagerSql = sql.ToString();
    }
}

public class SQL_TableVar : DbPagerBase
{
    internal override void CreateNextPageSQL()
    {








        StringBuilder sql = new StringBuilder();
        sql.Append(" declare @tt table(id int identity(1,1),nid int)");
        sql.Append(" insert into @tt(nid) select top {1} ");
        sql.Append(dbPager.primaryKey);
        sql.Append(" from ");
        sql.Append(dbPager.tableName);

        if (!string.IsNullOrEmpty(dbPager.whereSql))
        {
            sql.Append(" where ");
            sql.Append(dbPager.whereSql);
        }

        sql.Append(" order by ");
        sql.Append(dbPager.orderSql);

        sql.Append(" select  ");
        sql.Append(dbPager.field);
        sql.Append("  from ");
        sql.Append(dbPager.tableName);
        sql.Append(" t1, @tt t2 where t1.");
        sql.Append(dbPager.primaryKey);
        sql.Append(" =t2.nid and t2.id between {0} and {1}  order by t2.id");


        dbPager.pagerSql = sql.ToString();
    }
}

public class SQL_TopTop : DbPagerBase
{
    string _tableNames = "";                string _query = "";                    string _idColumn = "";
    string _orderCol = "";           string _orderColA = "";          string _orderColB = "";
    string[] _arrOrderCol;

    private void TopTopInit(string t1, string t2)
    {
        if (_tableNames.Length == 0)
        {
            _tableNames = dbPager.tableName;                        _query = dbPager.whereSql;
            _idColumn = dbPager.primaryKey.ToLower().Trim();

            _arrOrderCol = dbPager.orderSql.ToLower().Split(',');
        }

        string tmpCol;
        _orderCol = "";               _orderColA = "";              _orderColB = "";
        foreach (string a in _arrOrderCol)
        {
            if (a.Contains("desc"))
            {
                tmpCol = a.Replace("desc", "").Trim();
                _orderColA += t1 + tmpCol + " desc,";
                _orderColB += t2 + tmpCol + ",";
                _orderCol += tmpCol + ",";
            }
            else
            {
                tmpCol = a.Replace("asc", "").Trim();
                _orderColA += t1 + tmpCol + ",";
                _orderColB += t2 + tmpCol + " desc,";
                _orderCol += tmpCol + ",";
            }
        }

        _orderColA = _orderColA.TrimEnd(',');
        _orderColB = _orderColB.TrimEnd(',');
        _orderCol = _orderCol.TrimEnd(',');

        if (!_orderCol.Contains(_idColumn))                    _orderCol += "," + _idColumn;

    }

    internal override void CreateNextPageSQL()
    {
        int pageSize = dbPager.pagerSize;
        string tableShowColumns = dbPager.field;


        TopTopInit("", "t.");


        StringBuilder sql = new StringBuilder();
        sql.Append(" select ");
        sql.Append(tableShowColumns);
        sql.Append(" from ");             sql.Append(_tableNames);                    sql.Append(" where ");
        sql.Append(_idColumn);                 sql.Append(" in ( ");

        sql.Append(" select top ");
        sql.Append(pageSize);                       sql.Append(" ");                  sql.Append(_idColumn);                        sql.Append("  from (");

        sql.Append(" select top {0} ");
        sql.Append(_orderCol);                       sql.Append(" from ");
        sql.Append(_tableNames);
        if (!string.IsNullOrEmpty(_query) && _query.Length > 1)
        {   sql.Append(" where ");
            sql.Append(_query);
        }

        sql.Append(" order by ");                 sql.Append(_orderColA);
        sql.Append(" ) as t order by ");
        sql.Append(_orderColB);
        sql.Append(" ) ");

        if (!string.IsNullOrEmpty(_query) && _query.Length > 1)
        {   sql.Append(" and ");
            sql.Append(_query);
        }

        sql.Append(" order by ");
        sql.Append(_orderColA);
        dbPager.pagerSql = sql.ToString();
    }

    internal override void CreateLastPageSQL()
    {
        string tableShowColumns = dbPager.field;


        TopTopInit("t.", "");


        StringBuilder sql = new StringBuilder();
        sql.Append(" select ");
        sql.Append(tableShowColumns);               sql.Append(" from ( select top {0} * from ");
        sql.Append(_tableNames);
        if (!string.IsNullOrEmpty(_query) && _query.Length > 1)
        {   sql.Append(" where ");
            sql.Append(_query);
        }

        sql.Append(" order by ");                 sql.Append(_orderColB);
        sql.Append(" ) as t order by ");
        sql.Append(_orderColA);
        dbPager.lastPagerSql = sql.ToString();
    }

    internal override string GetSQLByPageIndex()
    {
        if (dbPager.pagerIndex < 1)
            dbPager.pagerIndex = 1;

        if (dbPager.pagerIndex > dbPager.pagerCount)
            dbPager.pagerIndex = dbPager.pagerCount;


        if (dbPager.pagerIndex == 1)
        {
            return dbPager.firstPagerSql;
        }
        int pagerlastcount = dbPager.pagerCount % dbPager.pagerSize == 0 ? dbPager.pagerCount / dbPager.pagerSize : dbPager.pagerCount / dbPager.pagerSize + 1;
        if (dbPager.pagerIndex == pagerlastcount)
        {
            Int32 p1 = dbPager.pagerCount % dbPager.pagerSize;
            if (p1 == 0)
                p1 = dbPager.pagerSize;

            return string.Format(dbPager.lastPagerSql, p1);
        }
        else
        {
            Int32 p1 = dbPager.pagerSize * dbPager.pagerIndex;
            return string.Format(dbPager.pagerSql, p1);
        }
    }
}

public class DbPager
{
    public string field;

    public string primaryKey;

    public string tableName;

    public string appendSql;

    public string whereSql;

    public string orderSql;

    public int pagerSize;

    public int pagerIndex;

    public int pagerCount;

    public DbHelper _db = null;

    public List<DbParameter> sqlParams = new List<DbParameter>();

    public DbPagerKind kind = DbPagerKind.Row_Number;

    public string pagerCountSql;

    public bool isDefaultRecordCount = true;

    public string pagerSql;

    public string firstPagerSql;

    public string lastPagerSql;

    public DbPager()
    {
        _db = new DbHelper();
    }

    public DbPager(DbHelper db)
    {
        _db = db;
    }

    public DbPager(DbHelper db, string field, string primaryKey, string tableName, string orderSql, int pagerSize, int pagerIndex, DbPagerKind kind)
    {
        _db = db;
        this.field = field;
        this.primaryKey = primaryKey;
        this.tableName = tableName;
        this.orderSql = orderSql;
        this.pagerSize = pagerSize;
        this.pagerIndex = pagerIndex;
        this.kind = kind;
    }

    private DbPagerBase dbPagerBase = null;

    private void CreateDbPagerBase()
    {
        switch (kind)
        {
        case DbPagerKind.Row_Number:
            dbPagerBase = new SQL_Row_Number();
            break;

        case DbPagerKind.TableVar:
            dbPagerBase = new SQL_TableVar();
            break;

        case DbPagerKind.MaxMin:
            dbPagerBase = new SQL_Max();
            break;

        case DbPagerKind.TopTop:
            dbPagerBase = new SQL_TopTop();
            break;

        case DbPagerKind.Max_TopTop:
            if (orderSql.Contains(","))
                dbPagerBase = new SQL_TopTop();
            else
                dbPagerBase = new SQL_Max();
            break;

        case DbPagerKind.Max_TableVar:
            if (orderSql.Contains(","))
                dbPagerBase = new SQL_TableVar();
            else
                dbPagerBase = new SQL_Max();
            break;
        }
        dbPagerBase.dbPager = this;
    }

    private void CreateSql()
    {
        if (dbPagerBase == null)
        {
            CreateDbPagerBase();
        }
        dbPagerBase.CreateRecordCountSQL();
        dbPagerBase.CreateFirstPageSQL();
        dbPagerBase.CreateNextPageSQL();
        dbPagerBase.CreateLastPageSQL();
    }

    public DataTable GetData(out int rowCount)
    {
        DataTable dt = GetData();
        rowCount = pagerCount;
        return dt;
    }

    public DataTable GetData()
    {
        CreateSql();
        bool ist = _db.IsBeginTransaction;
        if (!ist)
        {
            _db.BeginTransaction();
        }
        try
        {
            string pcount = string.Empty;

            if (sqlParams.Count > 0)
            {
                pcount = _db.ExecuteScalar<string>(pagerCountSql, sqlParams.ToArray());
                _db.ClearParameters();
            }
            else
            {
                pcount = _db.ExecuteScalar<string>(pagerCountSql);
            }

            if (string.IsNullOrEmpty(pcount))
            {
                pcount = "0";
            }
            pagerCount = int.Parse(pcount);

            string sql = dbPagerBase.GetSQLByPageIndex();
            if (sqlParams.Count > 0)
            {
                return _db.GetDataTable(sql, sqlParams.ToArray());
            }
            else
            {
                return _db.GetDataTable(sql);
            }
        }
        catch
        {
            _db.RollbackTransaction();
            throw;
        }
        finally
        {
            if (!ist)
            {
                _db.CommitTransaction();
            }
        }
    }
}
}
