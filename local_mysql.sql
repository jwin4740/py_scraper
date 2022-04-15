create database vaers;
use vaers;

select COUNT(VAERS_ID) from reports;

select COUNT(VAERS_ID) from vaccine;

select * from vaccine v ;
select * from reports r ;

select r.VAERS_ID, r.AGE_YEARS, r.VACCINATION_DATE, r.DIED, r.DATE_DIED, r.LAB_DATA, r.SYMPTOM_TEXT, r.PATIENT_HISTORY 
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID 
where v.VACCINATION_TYPE = 'COVID19' and r.DIED = 1 and r.AGE_YEARS < 30;


-- stomach or gastro
select r.VAERS_ID, r.AGE_YEARS, v.VACCINATION_MANUFACTURER, r.VACCINATION_DATE, r.AE_ONSET_DATE, r.SYMPTOM_TEXT, r.LAB_DATA, r.PATIENT_HISTORY, r.OTHER_MEDICATIONS
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID 
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%stomach%' or r.SYMPTOM_TEXT like '%gastr%');

-- breast feeding
select r.VAERS_ID, r.AGE_YEARS, v.VACCINATION_MANUFACTURER, r.VACCINATION_DATE, r.AE_ONSET_DATE, r.SYMPTOM_TEXT, r.LAB_DATA, r.PATIENT_HISTORY, r.OTHER_MEDICATIONS
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID 
where v.VACCINATION_TYPE = 'COVID19' and r.AGE_YEARS < 50 and (r.SYMPTOM_TEXT like '%breast%');

-- rash petechia
select r.VAERS_ID, r.AGE_YEARS, v.VACCINATION_MANUFACTURER, r.VACCINATION_DATE, r.AE_ONSET_DATE, r.SYMPTOM_TEXT, r.LAB_DATA, r.PATIENT_HISTORY, r.OTHER_MEDICATIONS
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID 
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%rash%');   

-- rash count
select count(r.VAERS_ID)
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID 
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%rash%');


-- suicide
select r.VAERS_ID, r.AGE_YEARS, r.VACCINATION_DATE, v.VACCINATION_TYPE, r.DIED, r.DATE_DIED, r.SYMPTOM_TEXT, r.LAB_DATA, r.PATIENT_HISTORY 
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID 
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%suicide%');

-- neurological
select r.VAERS_ID, r.AGE_YEARS, r.VACCINATION_DATE, r.REPORT_RECEIVED_DATE, r.DIED, r.DATE_DIED, r.SYMPTOM_TEXT, r.LAB_DATA, r.PATIENT_HISTORY 
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID 
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%tingling%');

-- 23673 tingling
select count(r.VAERS_ID)
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID 
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%tingling%');

-- crohn's disease OR gerd
select r.VAERS_ID, r.AGE_YEARS, r.VACCINATION_DATE, v.VACCINATION_TYPE, r.DIED, r.DATE_DIED, r.SYMPTOM_TEXT, r.LAB_DATA, r.PATIENT_HISTORY 
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID 
where v.VACCINATION_TYPE = 'COVID19' and r.DIED = 1 and (r.PATIENT_HISTORY like '%crohn%' or r.PATIENT_HISTORY like '%gerd%');

-- guillain barre
select r.VAERS_ID, r.AGE_YEARS, r.VACCINATION_DATE, v.VACCINATION_TYPE, r.DIED, r.DATE_DIED, r.SYMPTOM_TEXT, r.LAB_DATA, r.PATIENT_HISTORY 
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID 
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%barre%');

select r.VAERS_ID, r.AGE_YEARS, v.VACCINATION_MANUFACTURER, r.VACCINATION_DATE, r.AE_ONSET_DATE, r.SYMPTOM_TEXT, r.LAB_DATA, r.PATIENT_HISTORY, r.OTHER_MEDICATIONS
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID 
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%barre%') and r.VACCINATION_DATE BETWEEN CAST('2021-01-01' AS DATE) AND CAST('2021-05-28' AS DATE); 

select count(r.VAERS_ID)
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID 
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%barre%') and r.VACCINATION_DATE BETWEEN CAST('2021-01-01' AS DATE) AND CAST('2021-05-28' AS DATE); 

-- 700
select count(r.VAERS_ID)
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID 
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%guill%') and r.VACCINATION_DATE BETWEEN CAST('2021-01-01' AS DATE) AND CAST('2021-05-28' AS DATE); 

-- 1027
select count(r.VAERS_ID)
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID 
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%guill%'); 




select count(r.VAERS_ID)
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID 
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%barre%');



-- creutzfeldt jakob disease
select r.VAERS_ID, r.AGE_YEARS, r.VACCINATION_DATE, v.VACCINATION_TYPE, r.DIED, r.DATE_DIED, r.SYMPTOM_TEXT, r.LAB_DATA, r.PATIENT_HISTORY 
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID 
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%creutz%' or r.SYMPTOM_TEXT like '%cjd%');
  
 