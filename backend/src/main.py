import os,logging
from contextlib import asynccontextmanager
from fastapi import FastAPI,Request
from fastapi.responses import JSONResponse
from src.config.database import Base,engine,Session
from src.models import entities
from src.routes.api import router
from src.middlewares.request import RequestId,SecurityHeaders
from src.services import sync
@asynccontextmanager
async def lifespan(app):
 Base.metadata.create_all(engine)
 if os.getenv('SEED_DEMO')=='true':
  try:
   from scripts.seed_demo import seed
   seed()
  except Exception:logging.exception('seed failed')
 # 货损汇总发送线程：源端写入不等待接收端，断线只影响 outbox 状态
 with Session() as s:
  try:sync.backfill(s)
  except Exception:logging.exception('sync backfill failed')
 if os.getenv('SYNC_DISPATCHER','true').lower()=='true':sync.start_dispatcher()
 yield
 sync.stop_dispatcher()
app=FastAPI(title='冷链温控与损耗关联',lifespan=lifespan)
app.add_middleware(RequestId);app.add_middleware(SecurityHeaders)
app.include_router(router)
@app.get('/health')
def health():
 from sqlalchemy import text
 import redis
 with engine.connect() as c:c.execute(text('SELECT 1'))
 redis.Redis.from_url(os.environ['REDIS_URL']).ping()
 return {'status':'ok'}
@app.exception_handler(Exception)
async def errors(request:Request,exc:Exception):
 logging.exception('request failed');return JSONResponse(status_code=500,content={'detail':'服务暂时异常，请稍后重试'})
