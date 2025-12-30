import json
import logging
import os
import urllib.parse
from typing import Dict, Any
import boto3
from botocore.exceptions import ClientError

from database.contest_repository import ContestRepository

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3 클라이언트
s3_client = boto3.client('s3')


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    # DB Writer Lambda 핸들러
    # S3에 업로드된 크롤링 데이터를 읽어서 RDS에 저장
    # S3 이벤트에 의해 자동으로 트리거됨
    
    repository = None
    
    try:
        logger.info(f"Lambda 호출: {json.dumps(event, ensure_ascii=False)}")
        
        # DB 연결 설정
        db_config = get_db_config()
        repository = ContestRepository(db_config)
        
        # 1. 마감된 공모전 삭제 (일일 업데이트 시에만)
        from datetime import date
        deleted_count = repository.delete_closed_contests(date.today())
        logger.info(f"마감된 공모전 {deleted_count}개 삭제")
        
        # 2. S3 이벤트 처리
        total_saved = 0
        total_duplicates = 0
        
        for record in event.get('Records', []):
            # S3 이벤트 정보 추출
            bucket = record['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(record['s3']['object']['key'])
            
            logger.info(f"S3 객체 처리: s3://{bucket}/{key}")
            
            # S3에서 데이터 읽기
            contests_data = read_s3_object(bucket, key)
            
            # DB에 저장
            saved, duplicates = save_contests_to_db(repository, contests_data)
            total_saved += saved
            total_duplicates += duplicates
        
        logger.info(f"[METRICS] DB 저장 완료: 삭제={deleted_count}, 저장={total_saved}, 중복={total_duplicates}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'deleted': deleted_count,
                'total_saved': total_saved,
                'total_duplicates': total_duplicates
            }, ensure_ascii=False)
        }
        
    except Exception as e:
        logger.error(f"Lambda 실행 실패: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e)
            }, ensure_ascii=False)
        }
        
    finally:
        # 리소스 정리
        if repository:
            try:
                repository.close()
            except Exception as e:
                logger.error(f"Repository 정리 오류: {e}")


def get_db_config() -> Dict[str, Any]:
    # 환경 변수에서 DB 설정 가져오기
    required_keys = ['MYSQL_HOST', 'MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_DATABASE']
    missing = [k for k in required_keys if not os.environ.get(k)]
    
    if missing:
        raise Exception(
            f"필수 환경 변수 누락: {', '.join(missing)}"
        )
    
    try:
        db_port = int(os.environ.get('MYSQL_PORT', 3306))
    except Exception:
        raise Exception('MYSQL_PORT는 정수여야 합니다')
    
    return {
        'host': os.environ.get('MYSQL_HOST'),
        'port': db_port,
        'user': os.environ.get('MYSQL_USER'),
        'password': os.environ.get('MYSQL_PASSWORD'),
        'database': os.environ.get('MYSQL_DATABASE')
    }


def read_s3_object(bucket: str, key: str) -> Dict[str, Any]:
    # S3에서 JSON 객체 읽기
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        
        logger.info(f"S3 객체 읽기 성공: {data.get('total_contests', 0)}개 공모전")
        return data
        
    except ClientError as e:
        logger.error(f"S3 객체 읽기 실패: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"JSON 파싱 실패: {e}")
        raise


def save_contests_to_db(repository: ContestRepository, data: Dict[str, Any]) -> tuple:
    # 공모전 데이터를 DB에 저장 (배치 처리)
   
    contests = data.get('contests', [])
    event_type = data.get('event_type', 'unknown')
    
    logger.info(f"DB 저장 시작: {len(contests)}개 공모전 (이벤트: {event_type})")
    
    # 배치 저장 사용 (성능 최적화)
    saved_count, duplicate_count = repository.save_batch(contests)
    
    logger.info(f"DB 저장 완료: 저장={saved_count}, 중복={duplicate_count}")
    return saved_count, duplicate_count

