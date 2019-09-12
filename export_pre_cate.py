from fastapi import FastAPI, Form
from pydantic import BaseModel, Schema
from typing import List
from loguru import logger
import os
import cx_Oracle
import traceback
import redis

class HsCode(BaseModel):
    goodName: str = Schema(None, title="商品名称")
    hsCode: str = Schema(None, title="hs编码")
    count: int = Schema(..., gt=0, title="使用次数")

class Result(BaseModel):
    success: bool = Schema(None, title="是否找到商品true找到，false没有")
    msg: str = Schema(None, title="信息描述,错误原因等")
    expire: str = Schema(None, title="token过期时间")
    data: List[HsCode] = Schema([], title="商品信息列表")

username = os.getenv('ORCL_USERNAME') or 'username'
password = os.getenv('ORCL_PASSWORD') or 'password'
dbUrl = os.getenv('ORCL_DBURL') or '127.0.0.1:1521/orcl'
redisHost = os.getenv('REDIS_HOST') or '127.0.0.1'
redisPort = os.getenv('REDIS_PORT') or 6379
urlPrefix = os.getenv('URL_PREFIX') or '/maintain/export-pre-cate/'

pool = redis.ConnectionPool(host=redisHost, port=redisPort)
rconn = redis.Redis(connection_pool=pool)

app = FastAPI(
            title="出口商品预归类接口",
            description="这是一个简单的接口，根据商品名称可以查询出所有申报过这种品名的商品所使用的hs编码，及使用次数",
            version="1.0",
            openapi_url="{}openapi.json".format(urlPrefix),
            docs_url="{}docs".format(urlPrefix), redoc_url="{}redoc".format(urlPrefix)
)


def executeSql(sql, fetch=True, **kw):
    logger.info("sql is {}".format(sql))
    con = cx_Oracle.connect(username, password, dbUrl)
    cursor = con.cursor()
    result = None
    try:
        cursor.prepare(sql)
        cursor.execute(None, kw)
        if fetch:
            result = cursor.fetchall()
        else:
            con.commit()
    except Exception:
        traceback.print_exc()
        con.rollback()
    finally:
        cursor.close()
        con.close()
    return result


@app.post("{}getHsCode".format(urlPrefix), response_model=Result,
          summary="根据品名获取预归类信息",
          description="提供访问token,没有则申请，再提供一个品名，可以查询到这个品名预归类情况，每个商编使用了多少次")
async def get_hs_code(*, goodName: str = Form(..., title="商品名称", example="3D按摩器"), token: str = Form(..., title="访问token", example="aa7aa8a8fa604c60866413f52563b70c")):
    logger.info("goodName is {} token is {}".format(goodName, token))
    if rconn.get(token) is None:
        return Result(success=False, msg="token[{}]不存在，或者过期，请重新申请!".format(token))

    tokenExpire = rconn.ttl(token)
    tokenExpireDesc = "永久" if tokenExpire is None or tokenExpire == -1 else "{}s".format(tokenExpire)

    sql = '''
    select t1.g_name, t1.g_code, count(1) from ceb3_invt_head t
inner join ceb3_invt_list t1 on t1.head_guid = t.head_guid
where t.app_status in ('399', '800', '899')
and t1.g_name = :goodName
group by t1.g_name, t1.g_code
order by t1.g_name, t1.g_code, count(1) desc
    '''
    sqlResult = executeSql(sql, goodName=goodName)

    if sqlResult is None or len(sqlResult) == 0:
        return Result(success=False, msg="没有找到商品名称[{}]的申报数据!".format(goodName), expire=tokenExpireDesc)
    else:
        result = Result(success=True, msg="获取商品[{}]hs编码成功!".format(goodName), expire=tokenExpireDesc)
        for hc in sqlResult:
            result.data.append(HsCode(goodName=hc[0], hsCode=hc[1], count=hc[2]))

        return result

