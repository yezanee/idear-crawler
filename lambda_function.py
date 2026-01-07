import json
import logging
import os
from typing import Dict, Any
from datetime import datetime
import boto3

from crawler.linkareer_crawler import LinkareerCrawler
from utils.s3_uploader import S3Uploader

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# CloudWatch 클라이언트 (모니터링을 위해)
cloudwatch = boto3.client('cloudwatch')


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    # Lambda 핸들러 - 크롤링 후 S3에 저장
    # 이벤트 타입: initial_backfill, daily_update
    crawler = None
    
    try:
        logger.info(f"Lambda 호출: {json.dumps(event, ensure_ascii=False)}")
        
        # 이벤트 타입 추출
        event_type = event.get('type', 'daily_update')
        
        # S3 버킷 이름 확인
        s3_bucket = os.environ.get('S3_BUCKET_NAME')
        if not s3_bucket:
            raise Exception("환경 변수 S3_BUCKET_NAME이 설정되지 않았습니다.")
        
        # Crawler 및 S3 Uploader 초기화
        crawler = LinkareerCrawler()
        s3_uploader = S3Uploader(s3_bucket)
        
        # 이벤트 타입별 처리
        if event_type == 'initial_backfill':
            result = handle_initial_backfill(crawler, s3_uploader, event)
        elif event_type == 'daily_update':
            result = handle_daily_update(crawler, s3_uploader, event)
        else:
            return error_response(f"알 수 없는 이벤트 타입: {event_type}", 400)
        
        # CloudWatch 메트릭 전송
        publish_metrics(event_type, result)
        
        logger.info(f"크롤링 완료: {result}")
        return success_response(result)
        
    except Exception as e:
        logger.error(f"Lambda 실행 실패: {str(e)}", exc_info=True)
        return error_response(str(e), 500)
        
    finally:
        # 리소스 정리
        if crawler:
            try:
                crawler.cleanup()
            except Exception as e:
                logger.error(f"Crawler 정리 오류: {e}")


def handle_initial_backfill(crawler: LinkareerCrawler, s3_uploader: S3Uploader, event: Dict) -> Dict[str, Any]:
    # 초기 백필 처리
    max_pages = event.get('max_pages')
    
    if max_pages:
        logger.info(f"초기 백필 시작 (최대 {max_pages}페이지)")
    else:
        logger.info("초기 백필 시작 (전체 페이지)")
        logger.warning("max_pages 미지정 - Lambda 타임아웃(15분) 주의")

    # 크롤링 실행
    result = crawler.initial_backfill(max_pages=max_pages)
    contests = result['contests']
    
    # S3에 업로드
    if contests:
        s3_key = s3_uploader.upload_contests(contests, 'initial_backfill')
        logger.info(f"S3 업로드 완료: {s3_key}")
    else:
        logger.warning("수집된 데이터 없음 - S3 업로드 생략")
        s3_key = None
    
    # 메트릭 로깅
    logger.info(f"[METRICS] 초기 백필 완료: "
                f"수집={result['total_collected']}, "
                f"페이지={result['pages_processed']}, "
                f"시간={result['execution_time']}초")
    
    return {
        'type': 'initial_backfill',
        'total_collected': result['total_collected'],
        'pages_processed': result['pages_processed'],
        'execution_time': result['execution_time'],
        'max_pages': max_pages,
        's3_key': s3_key,
        'timestamp': datetime.now().isoformat()
    }


def handle_daily_update(crawler: LinkareerCrawler, s3_uploader: S3Uploader, event: Dict) -> Dict[str, Any]:
    # 일일 업데이트 처리
    logger.info("일일 업데이트 시작")
    
    # 크롤링 실행
    result = crawler.daily_update()
    contests = result['contests']
    
    # S3에 업로드
    if contests:
        s3_key = s3_uploader.upload_contests(contests, 'daily_update')
        logger.info(f"S3 업로드 완료: {s3_key}")
    else:
        logger.warning("수집된 데이터 없음 - S3 업로드 생략")
        s3_key = None
    
    # 메트릭 로깅
    logger.info(f"[METRICS] 일일 업데이트 완료: "
                f"수집={result['total_collected']}, "
                f"시간={result['execution_time']}초")
    
    return {
        'type': 'daily_update',
        'total_collected': result['total_collected'],
        'execution_time': result['execution_time'],
        's3_key': s3_key,
        'timestamp': datetime.now().isoformat()
    }


def success_response(data: Any, status_code: int = 200) -> Dict[str, Any]:
    # 성공 응답 생성
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps({
            'success': True,
            'data': data
        }, default=str, ensure_ascii=False)
    }


def error_response(message: str, status_code: int = 500) -> Dict[str, Any]:
    # 에러 응답 생성
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps({
            'success': False,
            'error': message
        }, ensure_ascii=False)
    }


def publish_metrics(event_type: str, result: Dict[str, Any]) -> None:
    # CloudWatch 메트릭 전송
    try:
        metrics = []
        
        # 수집된 공모전 개수
        if 'total_collected' in result:
            metrics.append({
                'MetricName': 'ContestsCollected',
                'Value': result['total_collected'],
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'EventType', 'Value': event_type}
                ]
            })
        
        # 실행 시간
        if 'execution_time' in result:
            metrics.append({
                'MetricName': 'ExecutionTime',
                'Value': result['execution_time'],
                'Unit': 'Seconds',
                'Dimensions': [
                    {'Name': 'EventType', 'Value': event_type}
                ]
            })
        
        # 처리된 페이지 수
        if 'pages_processed' in result:
            metrics.append({
                'MetricName': 'PagesProcessed',
                'Value': result['pages_processed'],
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'EventType', 'Value': event_type}
                ]
            })
        
        # 메트릭 전송
        if metrics:
            cloudwatch.put_metric_data(
                Namespace='IdearCrawler',
                MetricData=metrics
            )
            logger.info(f"CloudWatch 메트릭 전송 완료: {len(metrics)}개")
    
    except Exception as e:
        logger.warning(f"CloudWatch 메트릭 전송 실패: {e}")

