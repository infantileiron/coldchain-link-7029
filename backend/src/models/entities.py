from sqlalchemy import Column,Integer,String,Float,JSON,Text,BigInteger,UniqueConstraint
from src.config.database import Base
class User(Base):
 __tablename__='users'
 id=Column(Integer,primary_key=True);email=Column(String(100),unique=True);password=Column(String(200));role=Column(String(20))
class Shipment(Base):
 __tablename__='shipments'
 id=Column(Integer,primary_key=True);code=Column(String(60),unique=True);route=Column(String(60));zone=Column(String(20));driver=Column(String(60))
class Sample(Base):
 __tablename__='samples'
 id=Column(Integer,primary_key=True);shipment_id=Column(Integer,index=True);code=Column(String(60),unique=True);exposure=Column(Float,nullable=True);excursions=Column(Integer,nullable=True);loss=Column(Float,nullable=True)
class Study(Base):
 __tablename__='studies'
 id=Column(Integer,primary_key=True);name=Column(String(100));zone=Column(String(20));feature=Column(String(30));state=Column(String(20),default='queued');result=Column(JSON,nullable=True);error=Column(Text,default='')
class Recommendation(Base):
 __tablename__='recommendations'
 id=Column(Integer,primary_key=True);study_id=Column(Integer);note=Column(Text);owner=Column(String(60));status=Column(String(20),default='draft')
class Audit(Base):
 __tablename__='audits'
 id=Column(Integer,primary_key=True);actor=Column(String(100));action=Column(String(200));created=Column(String(40))

# ---- 线路货损汇总 -> 独立接收端 ----
# 源库事实：每次样本/班次写入都落一条源版本（source_seq 单调递增），
# sync_group 记录每个组已发送到哪个版本，sync_outbox 记录待发送/待确认/已对账。
class SourceVersion(Base):
 __tablename__='source_versions'
 id=Column(Integer,primary_key=True);seq=Column(BigInteger,nullable=False,unique=True);entity=Column(String(20));entity_id=Column(Integer);action=Column(String(20));created=Column(String(40))
class SyncCounter(Base):
 __tablename__='sync_counter'
 id=Column(Integer,primary_key=True);value=Column(BigInteger,default=0,nullable=False)
class SyncGroup(Base):
 # 组键 = route|zone；每个组上一份已推送快照，用于识别更正是否需要跨组迁移
 __tablename__='sync_groups'
 id=Column(Integer,primary_key=True);route=Column(String(60));zone=Column(String(20));sent_seq=Column(BigInteger,default=0,nullable=False);sent_payload=Column(JSON,nullable=True);updated=Column(String(40))
 __table_args__=(UniqueConstraint('route','zone',name='uq_sync_group'),)
class SyncOutbox(Base):
 # key = 组键；version = 该组已入账的最新源版本；payload = 该版本下该组的汇总
 # state: pending 待发送 / sending 已发出待回执 / needs_resend 待确认
 #        / diverged 两端版本不一致待核对 / reconciled 已对账
 __tablename__='sync_outbox'
 id=Column(Integer,primary_key=True);key=Column(String(90),unique=True,nullable=False);route=Column(String(60));zone=Column(String(20));version=Column(BigInteger,default=0,nullable=False);payload=Column(JSON,nullable=False)
 state=Column(String(20),default='pending');attempts=Column(Integer,default=0);last_error=Column(Text,default='')
 receipt_version=Column(BigInteger,nullable=True);ledger_version=Column(BigInteger,nullable=True);ledger_payload=Column(JSON,nullable=True)
 created=Column(String(40));updated=Column(String(40));last_attempt=Column(String(40),nullable=True)
