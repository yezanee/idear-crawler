import logging
from typing import Dict, Optional
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class ContestDetailParser:
    # 공모전 상세 페이지 파서
    
    def __init__(self):
        self.session = requests.Session()

        # Retry 정책 적용
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                         'AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/120.0.0.0 Safari/537.36'
        })
    
    def parse_detail_page(self, url: str) -> Dict:
        # 상세 페이지 크롤링

        try:
            logger.debug(f"상세 페이지 크롤링: {url}")
            
            # HTML 가져오기
            response = self.session.get(url, timeout=10)
            
            # 응답 검증
            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}")
            
            # 파싱 준비
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 기본 정보 추출
            title = self._get_text_safely(soup, 'h1')
            
            # 유효성 검증
            if not title or title == '링커리어':
                raise Exception("Invalid page (deleted or not found)")
            
            host = self._get_text_safely(soup, 'h2.organization-name')
            category = self._get_text_safely(soup, 
                'ul.CategoryChipList__StyledWrapper-sc-756dba5c-0 li p')
            image_url = self._get_attr_safely(soup, 'img.card-image', 'src')
            reward = self._get_field_value(soup, '시상규모')
            homepage_url = self._get_homepage_url(soup)
            description = self._get_description(soup)
            start_date, deadline = self._parse_date_range(soup)
            
            # 최종 결과 구성
            contest_data = {
                'title': title,
                'host': host,
                'category': category,
                'image_url': image_url,
                'start_date': start_date,
                'deadline': deadline,
                'reward': reward,
                'description': description,
                'linkareer_url': url,
                'homepage_url': homepage_url
            }
            
            logger.debug(f"크롤링 완료: {title}")
            return contest_data
            
        except Exception as e:
            logger.error(f"상세 페이지 크롤링 실패: {url} - {str(e)}")
            raise
    
    # 안전 추출 유틸 (실패해도 None 반환)

    def _get_text_safely(self, soup: BeautifulSoup, selector: str) -> Optional[str]:
        # 안전한 텍스트 추출
        try:
            element = soup.select_one(selector)
            if element:
                text = element.get_text(strip=True)
                return text if text else None
            return None
        except Exception as e:
            logger.warning(f"텍스트 추출 실패 ({selector}): {e}")
            return None
    
    def _get_attr_safely(self, soup: BeautifulSoup, selector: str, 
                        attr: str) -> Optional[str]:
        # 안전한 속성 추출
        try:
            element = soup.select_one(selector)
            if element:
                value = element.get(attr)
                # 상대 경로를 절대 경로로 변환
                if value and not value.startswith('http'):
                    if value.startswith('//'):
                        value = 'https:' + value
                    elif value.startswith('/'):
                        value = 'https://api.linkareer.com' + value
                return value
            return None
        except Exception as e:
            logger.warning(f"속성 추출 실패 ({selector}[{attr}]): {e}")
            return None
    
    def _get_field_value(self, soup: BeautifulSoup, field_label: str) -> Optional[str]:
        # dt-dd 구조에서 특정 필드값 추출
        try:
            dls = soup.select('dl')
            for dl in dls:
                dt = dl.select_one('dt.field-label')
                if dt and field_label in dt.get_text():
                    dd = dl.select_one('dd.text')
                    if dd:
                        text = dd.get_text(strip=True)
                        return text if text else None
            return None
        except Exception as e:
            logger.warning(f"필드 값 추출 실패 ({field_label}): {e}")
            return None
    
    def _get_homepage_url(self, soup: BeautifulSoup) -> Optional[str]:
        # 홈페이지 URL 추출
        try:
            dls = soup.select('dl')
            for dl in dls:
                dt = dl.select_one('dt.field-label')
                if dt and '홈페이지' in dt.get_text():
                    dd = dl.select_one('dd.text')
                    if dd:
                        link = dd.select_one('a[href]')
                        if link:
                            href = link.get('href')
                            if href and href.strip():
                                return href.strip()
            return None
        except Exception as e:
            logger.warning(f"홈페이지 URL 추출 실패: {e}")
            return None
    
    def _get_description(self, soup: BeautifulSoup) -> Optional[str]:
        # 상세 설명 추출
        try:
            element = soup.select_one('div.responsive-element')
            if not element:
                return None
            
            # style, script 태그 제거
            for tag in element.select('style, script'):
                tag.decompose()
            
            # 텍스트 추출 (줄바꿈 유지)
            text = element.get_text(separator='\n', strip=True)
            
            # 텍스트 정리
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            result = '\n'.join(lines)
            
            # 길이 제한 (DB 컬럼 크기 고려)
            if len(result) > 5000:
                result = result[:5000] + '...'
            
            return result if result else None
            
        except Exception as e:
            logger.warning(f"설명 추출 실패: {e}")
            return None
    
    def _parse_date_range(self, soup: BeautifulSoup) -> tuple:
        # 접수기간에서 시작일과 마감일 추출
        start_date = None
        deadline = None
        
        try:
            dls = soup.select('dl')
            for dl in dls:
                dt = dl.select_one('dt.field-label')
                if dt and '접수기간' in dt.get_text():
                    dd = dl.select_one('dd.text')
                    if dd:
                        # 시작일
                        start_span = dd.select_one('span.start-at')
                        if start_span:
                            next_span = start_span.find_next_sibling('span')
                            if next_span:
                                start_date = self._parse_date(
                                    next_span.get_text(strip=True)
                                )
                        
                        # 마감일
                        end_span = dd.select_one('span.end-at')
                        if end_span:
                            next_span = end_span.find_next_sibling('span')
                            if next_span:
                                deadline = self._parse_date(
                                    next_span.get_text(strip=True)
                                )
                        break
        except Exception as e:
            logger.warning(f"날짜 범위 파싱 실패: {e}")
        
        return start_date, deadline
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        # 날짜 문자열 파싱
        if not date_str or not date_str.strip():
            return None
        
        try:
            date_str = date_str.strip()
            
            # YYYY.M.D 형식 파싱
            if '.' in date_str:
                parts = date_str.split('.')
                if len(parts) == 3:
                    year = int(parts[0])
                    month = int(parts[1])
                    day = int(parts[2])
                    
                    # 유효성 검증
                    dt = datetime(year, month, day)
                    return dt.strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"날짜 파싱 실패: {date_str} - {e}")
        
        return None
    
    def cleanup(self):
        # 리소스 정리
        if hasattr(self, 'session'):
            try:
                self.session.close()
            except:
                pass
