import json
import logging
from datetime import datetime
from typing import Dict, List, Any
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Uploader:
    # 크롤링 데이터를 S3에 업로드하는 클래스
    
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
        logger.info(f"S3 Uploader 초기화: bucket={bucket_name}")
    
    def upload_contests(self, contests: List[Dict[str, Any]], event_type: str) -> str:
        # 크롤링한 공모전 데이터를 S3에 JSON 파일로 업로드
        try:
            # 타임스탬프 기반 파일명 생성
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            object_key = f"crawler-data/{event_type}/{timestamp}.json"
            
            # 메타데이터와 함께 JSON 생성
            data = {
                'event_type': event_type,
                'timestamp': datetime.now().isoformat(),
                'total_contests': len(contests),
                'contests': contests
            }
            
            # S3에 업로드
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=object_key,
                Body=json.dumps(data, ensure_ascii=False, indent=2),
                ContentType='application/json',
                Metadata={
                    'event_type': event_type,
                    'contest_count': str(len(contests))
                }
            )
            
            logger.info(f"S3 업로드 성공: s3://{self.bucket_name}/{object_key} ({len(contests)}개 공모전)")
            return object_key
            
        except ClientError as e:
            logger.error(f"S3 업로드 실패: {e}")
            raise
        except Exception as e:
            logger.error(f"예상치 못한 오류: {e}")
            raise
    
    def upload_batch(self, contests: List[Dict[str, Any]], batch_number: int, event_type: str) -> str:
        # 배치 단위로 데이터 업로드 (대량 데이터 처리용)
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            object_key = f"crawler-data/{event_type}/batch_{batch_number}_{timestamp}.json"
            
            data = {
                'event_type': event_type,
                'batch_number': batch_number,
                'timestamp': datetime.now().isoformat(),
                'total_contests': len(contests),
                'contests': contests
            }
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=object_key,
                Body=json.dumps(data, ensure_ascii=False, indent=2),
                ContentType='application/json',
                Metadata={
                    'event_type': event_type,
                    'batch_number': str(batch_number),
                    'contest_count': str(len(contests))
                }
            )
            
            logger.info(f"배치 {batch_number} S3 업로드 성공: {len(contests)}개 공모전")
            return object_key
            
        except Exception as e:
            logger.error(f"배치 업로드 실패: {e}")
            raise
