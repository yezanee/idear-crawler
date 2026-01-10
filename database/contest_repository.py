"""
공모전 데이터베이스 Repository
Real MySQL 8.0 원칙에 따른 최적화 버전
"""
import logging
from typing import Dict, List, Optional
from datetime import date
import pymysql
from pymysql.cursors import DictCursor
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class ContestRepository:
    """
    공모전 데이터베이스 Repository
    
    필수 조건:
    - contest 테이블에 linkareer_url UNIQUE INDEX가 설정되어 있어야 함
    - 스키마는 database/schema.sql 참조
    """
    
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.connection = None
        self._connect()
    
    def _connect(self):
        """데이터베이스 연결 생성"""
        try:
            self.connection = pymysql.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset='utf8mb4',
                cursorclass=DictCursor,
                autocommit=False,
                # Lambda 환경에 최적화된 타임아웃 설정
                connect_timeout=15,  # 초기 연결 타임아웃 증가 (VPC Cold Start 고려)
                read_timeout=60,     # 읽기 타임아웃 증가
                write_timeout=60     # 쓰기 타임아웃 증가
            )
            logger.info("Database connected successfully")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def _ensure_connection(self):
        """연결 상태 확인 및 재연결"""
        try:
            if self.connection is None or not self.connection.open:
                logger.warning("Database connection lost, reconnecting...")
                self._connect()
            else:
                # 연결 테스트
                self.connection.ping(reconnect=True)
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            self._connect()
    
    @contextmanager
    def _get_cursor(self):
        """커서 컨텍스트 매니저"""
        self._ensure_connection()
        cursor = self.connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
    
    # =========================================================================
    # 조회 메서드
    # =========================================================================
    
    def exists_by_url(self, linkareer_url: str) -> bool:
        """
        URL로 공모전 존재 여부 확인
        
        인덱스 활용: idx_linkareer_url (UNIQUE)
        - 커버링 인덱스로 동작하여 테이블 접근 없이 인덱스만으로 결과 반환
        """
        try:
            with self._get_cursor() as cursor:
                # EXISTS 사용 시 첫 번째 매칭에서 즉시 반환 (COUNT보다 효율적)
                sql = "SELECT EXISTS(SELECT 1 FROM contest WHERE linkareer_url = %s) as exist"
                cursor.execute(sql, (linkareer_url,))
                result = cursor.fetchone()
                return result['exist'] == 1
        except Exception as e:
            logger.error(f"URL 존재 확인 실패: {e}")
            return False
    
    def exists_by_homepage_url(self, homepage_url: str) -> bool:
        """
        홈페이지 URL로 공모전 존재 여부 확인
        
        인덱스 활용: idx_homepage_url
        """
        if not homepage_url or not homepage_url.strip():
            return False
        
        try:
            with self._get_cursor() as cursor:
                sql = "SELECT EXISTS(SELECT 1 FROM contest WHERE homepage_url = %s) as exist"
                cursor.execute(sql, (homepage_url,))
                result = cursor.fetchone()
                return result['exist'] == 1
        except Exception as e:
            logger.error(f"홈페이지 URL 존재 확인 실패: {e}")
            return False
    
    def count(self) -> int:
        """전체 공모전 개수 조회"""
        try:
            with self._get_cursor() as cursor:
                sql = "SELECT COUNT(*) as cnt FROM contest"
                cursor.execute(sql)
                result = cursor.fetchone()
                return result['cnt']
        except Exception as e:
            logger.error(f"공모전 개수 조회 실패: {e}")
            return 0
    
    # =========================================================================
    # 저장 메서드
    # =========================================================================
    
    def save_if_not_duplicate(self, contest_data: Dict) -> bool:
        """
        중복이 아닌 경우에만 공모전 저장 (단건 UPSERT)
        
        인덱스 활용: idx_linkareer_url (UNIQUE)
        - ON DUPLICATE KEY UPDATE 구문은 UNIQUE 인덱스가 필수
        """
        linkareer_url = contest_data.get('linkareer_url')
        if not linkareer_url:
            logger.warning("linkareer_url이 없습니다")
            return False
        
        try:
            with self._get_cursor() as cursor:
                sql = """
                    INSERT INTO contest 
                    (title, host, category, image_url, start_date, deadline, 
                     reward, description, linkareer_url, homepage_url, is_active)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                    ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    host = VALUES(host),
                    category = VALUES(category),
                    image_url = VALUES(image_url),
                    start_date = VALUES(start_date),
                    deadline = VALUES(deadline),
                    reward = VALUES(reward),
                    description = VALUES(description),
                    homepage_url = VALUES(homepage_url),
                    is_active = 1
                """
                
                cursor.execute(sql, (
                    contest_data.get('title'),
                    contest_data.get('host'),
                    contest_data.get('category'),
                    contest_data.get('image_url'),
                    contest_data.get('start_date'),
                    contest_data.get('deadline'),
                    contest_data.get('reward'),
                    contest_data.get('description'),
                    linkareer_url,
                    contest_data.get('homepage_url')
                ))
                
                self.connection.commit()
                logger.debug(f"공모전 저장/업데이트 성공: {contest_data.get('title')}")
                return True
                
        except Exception as e:
            logger.error(f"공모전 저장 실패: {e}", exc_info=True)
            try:
                self.connection.rollback()
            except:
                pass
            return False
    
    def save_batch(self, contests: List[Dict]) -> tuple:
        """
        배치로 공모전 저장 (성능 최적화)
        
        인덱스 활용: idx_linkareer_url (UNIQUE)
        - INSERT ... ON DUPLICATE KEY UPDATE 구문 사용
        - executemany로 배치 처리하여 네트워크 왕복 최소화
        
        Returns:
            tuple: (저장 성공 개수, 실패 개수)
        """
        if not contests:
            return 0, 0
        
        try:
            with self._get_cursor() as cursor:
                sql = """
                    INSERT INTO contest 
                    (title, host, category, image_url, start_date, deadline, 
                     reward, description, linkareer_url, homepage_url, is_active)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                    ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    host = VALUES(host),
                    category = VALUES(category),
                    image_url = VALUES(image_url),
                    start_date = VALUES(start_date),
                    deadline = VALUES(deadline),
                    reward = VALUES(reward),
                    description = VALUES(description),
                    homepage_url = VALUES(homepage_url),
                    is_active = 1
                """
                
                # URL이 없는 데이터는 제외
                values = [
                    (
                        c.get('title'),
                        c.get('host'),
                        c.get('category'),
                        c.get('image_url'),
                        c.get('start_date'),
                        c.get('deadline'),
                        c.get('reward'),
                        c.get('description'),
                        c.get('linkareer_url'),
                        c.get('homepage_url')
                    )
                    for c in contests if c.get('linkareer_url')
                ]
                
                if not values:
                    logger.warning("유효한 데이터가 없습니다 (linkareer_url 누락)")
                    return 0, 0
                
                cursor.executemany(sql, values)
                self.connection.commit()
                
                # affected_rows 설명:
                # - 1: 새 행 삽입
                # - 2: 기존 행 업데이트
                # - 0: 변경 없음 (데이터 동일)
                total_affected = cursor.rowcount
                logger.info(f"배치 저장 완료: {len(values)}개 시도, affected_rows={total_affected}")
                
                # UPSERT 방식이므로 모두 처리됨 (중복도 UPDATE로 처리)
                return len(values), 0
                
        except Exception as e:
            logger.error(f"배치 저장 실패: {e}", exc_info=True)
            try:
                self.connection.rollback()
            except:
                pass
            return 0, len(contests)
    
    # =========================================================================
    # 삭제 메서드
    # =========================================================================
    
    def delete_closed_contests(self, today: date) -> int:
        """
        마감된 공모전 삭제
        
        인덱스 활용: idx_deadline
        - 날짜 범위 검색에 B-Tree 인덱스 활용
        - 주의: 삭제 대상이 전체의 20% 이상이면 Full Scan이 더 효율적일 수 있음
        """
        try:
            with self._get_cursor() as cursor:
                sql = "UPDATE contest SET is_active = 0 WHERE deadline < %s"
                affected = cursor.execute(sql, (today,))
                self.connection.commit()
                logger.info(f"마감된 공모전 {affected}개 비활성화 (Soft Delete)")
                return affected
        except Exception as e:
            logger.error(f"마감 공모전 삭제 실패: {e}")
            try:
                self.connection.rollback()
            except:
                pass
            return 0
    
    # =========================================================================
    # 리소스 관리
    # =========================================================================
    
    def close(self):
        """데이터베이스 연결 종료"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Database close error: {e}")
