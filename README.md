# DbHelper

```c#
using Cat.Database;
```

## web.config

```xml
<connectionStrings>
	<add name="ConnectionString" connectionString="server=.,1791;uid=xxx;pwd=xxx;database=pubs;trusted_connection=no;MultipleActiveResultSets=true;" providerName="System.Data.SqlClient"/>
	<!--<add name="ConnectionString" connectionString="Provider=Microsoft.Jet.OLEDB.4.0; Data Source=|DataDirectory|d_b.mdb" providerName="System.Data.OleDb"/>-->
</connectionStrings>
```

## Usage

```c#
DbHelper db = new DbHelper();

Repeater1.DataSource = db.GetDataTable("select * from [jobs]");
Repeater1.DataBind();

SqlParameter par = new SqlParameter("@job_id", 1);
msg.Text = db.ExecuteScalar<string>("select job_desc from jobs where job_id=@job_id", par);
```

### Paging

```c#
DbPager pager = new DbPager();
pager.field = "*";
pager.tableName = "jobs";
pager.primaryKey = "job_id";
pager.orderSql = "job_id desc";
//pager.appendSql = "left join jobs on jobs.job_id=employee.job_id";
pager.pagerSize = Pager1.PageSize;
pager.pagerIndex = Pager1.CurPage;
//pager.kind = DbPagerKind.TableVar;
Repeater1.DataSource = pager.GetData();
Pager1.RecordCount = pager.pagerCount;
Repeater1.DataBind();
```

### Insert

```c#
DbInsert insert = new DbInsert();
insert.Add("job_desc", TextBox1.Text);
insert.Add("min_lvl", TextBox2.Text);
insert.Add("max_lvl", TextBox2.Text);
insert.Execute("jobs");
```

### Update

```c#
DbUpdate update = new DbUpdate();
update.Add("max_lvl", TextBox2.Text);
SqlParameter par = new SqlParameter("@job_id", 1);
update.Execute("jobs", "where job_id=@job_id", par);
```

### Transaction

```c#
DbHelper db = new DbHelper();
db.BeginTransaction();
try
{
	db.ExecuteNonQuery('sql...1');
	db.ExecuteNonQuery('sql...2');
	int a = 0;
    int b = 1 / a; //exception code
	db.ExecuteNonQuery('sql...3');
    db.CommitTransaction();
}
catch (Exception)
{
	db.RollbackTransaction();   
}

```
