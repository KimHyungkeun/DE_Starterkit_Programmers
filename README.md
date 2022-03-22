# Data_Engineer_Starterkit_7th_Homework

[1. Week2 : 월별로 기록된 유저 수를 기록한 테이블](https://github.com/KimHyungkeun/DE_Starterkit_Programmers/blob/main/week2_HW.ipynb)  
 - 월별로 접속한 유저 들의 수를 기록한 테이블을 만들어 낸다

[2. Week3 : 테이블 별 다양한 쿼리 과제 진행](https://github.com/KimHyungkeun/DE_Starterkit_Programmers/blob/main/week3_HW.ipynb)
 - Assignment 2 : 한 유저가 사이트를 접속하였을때, 해당 유저의 접속 timestamp기준으로 가장 먼저 접근했던 사이트와 가장 늦게 접근했던 사이트를 표시한다
 - Assignment 3 : Gross Revenue가 가장 큰 값을 찾는 과제. (매출을 낼 시 refund까지 모두 포함한 매출을 Gross Revenue라고 한다)
 - Assignment 4 : 월별 접속한 사이트를 기준으로 접속한 유저(uniqueUsers), 광고비를 낸 유저(paidUser), 접속 유저 중 광고비 낸 유저의 비율(ConverseionRate), Gross Revenue, Net Revenue를 구한다

[3. Week4 : 테이블 적재 코드 수정](https://github.com/KimHyungkeun/DE_Starterkit_Programmers/blob/main/week4_HW.ipynb) 
 - 테이블 Full Refresh를 구현한다.
 - 구현 방식은 기존 테이블 내용을 모두 Delete하고, 빈 테이블에 row별로 하나씩 INSERT 하는 방식이다.
 - 기존 테이블 내용을 Delete후, 새로 추가할 row갯수만큼 Insert 하므로 이 두가지 연산을 하나의 트랜잭션으로 묶는다

[4. Week5 : 테이블 Full Refresh 구현(Airflow DAG)](https://github.com/KimHyungkeun/DE_Starterkit_Programmers/blob/main/week5_assignment_full_refresh.py) 
 - 하루가 지날때마다, 지나간 하루를 시작점으로 하여 1주일간의 온도 정보를 불러온다.

[5. Week5 : 테이블 Incremental update 구현(Airflow DAG](https://github.com/KimHyungkeun/DE_Starterkit_Programmers/blob/main/week5_assignment_incermental_update.py) 
 - 하루가 지날때마다, 지나간 하루를 시작점으로 하는 1주일간의 온도 정보를 불러온다.
 - 시작일이 새로 덮어씌워지는것이 아니라, 하루가 지날때마다 row가 하나씩 늘어나는 방식이다

[6. Week6 : 일별 nps 테이블 구현(Airflow DAG)](https://github.com/KimHyungkeun/DE_Starterkit_Programmers/blob/main/week6_assignment_nps_summary_table.py) 
 - 일별로 nps를 구하는 테이블이다
 - nps는 하루 중 9, 10점을 받은 비율에서 0~6점을 받은 비율을 제외한 값이다.



