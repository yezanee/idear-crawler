# iDear-Crawler

블록체인 기반 아이디어 저장·공증 플랫폼(iDear)을 위한  
Python AWS Lambda 기반 데이터 수집 크롤러입니다.

<br>

## Overview

공모전 및 아이디어 데이터를 수집하여  
AI 분석 및 서비스 기능에 활용하기 위한 크롤링 시스템입니다.

기존 Selenium 기반 크롤링의 성능 및 안정성 문제를 해결하기 위해  
GraphQL 직접 조회 방식과 서버리스 구조로 재설계했습니다.

<br>

## Tech Stack

- Python
- AWS Lambda
- GraphQL

<br>

## Key Features

### 1. 데이터 수집 크롤링
- 공모전 데이터 수집
- 필요한 정보만 추출하여 가공

### 2. GraphQL 기반 데이터 조회
- 웹 페이지의 Network 요청을 분석하여 내부 GraphQL API 구조 파악
- HTML 파싱 대신 API를 직접 호출하여 필요한 데이터만 조회

### 3. 서버리스 실행 구조
- AWS Lambda 기반 크롤링
- 스케줄링을 통해 주기적으로 데이터 수집

<br>

## Performance Improvement
- Selenium 기반 크롤링 → GraphQL 기반 구조로 개선
- 실행 시간: **3시간 이상 → 약 10초 이내로 단축**
- 서버 다운 문제 해결 및 리소스 사용량 감소
