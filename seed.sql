create database vaers;
use vaers;

select COUNT(VAERS_ID) from reports;

select COUNT(VAERS_ID) from vaccine;

select * from vaccine v ;
select * from reports r ;

select r.VAERS_ID, r.AGE_YEARS, r.VACCINATION_DATE, r.DIED, r.DATE_DIED, r.LAB_DATA, r.SYMPTOM_TEXT, r.PATIENT_HISTORY
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID
where v.VACCINATION_TYPE = 'COVID19' and r.DIED = 1 and r.AGE_YEARS < 50;

-- stomach or nausea
select r.VAERS_ID, r.AGE_YEARS, r.VACCINATION_DATE, v.VACCINATION_TYPE, r.DIED, r.DATE_DIED, r.SYMPTOM_TEXT, r.LAB_DATA, r.PATIENT_HISTORY
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID
where v.VACCINATION_TYPE = 'COVID19' and r.DIED = 1 and (r.SYMPTOM_TEXT like '%stomach%' or r.SYMPTOM_TEXT like '%nausea%');

-- suicide
select r.VAERS_ID, r.AGE_YEARS, r.VACCINATION_DATE, v.VACCINATION_TYPE, r.DIED, r.DATE_DIED, r.SYMPTOM_TEXT, r.LAB_DATA, r.PATIENT_HISTORY
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%suicide%');

-- neurological
select r.VAERS_ID, r.AGE_YEARS, r.VACCINATION_DATE, v.VACCINATION_TYPE, r.DIED, r.DATE_DIED, r.SYMPTOM_TEXT, r.LAB_DATA, r.PATIENT_HISTORY
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%neuro%');

-- crohn's disease OR gerd
select r.VAERS_ID, r.AGE_YEARS, r.VACCINATION_DATE, v.VACCINATION_TYPE, r.DIED, r.DATE_DIED, r.SYMPTOM_TEXT, r.LAB_DATA, r.PATIENT_HISTORY
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID
where v.VACCINATION_TYPE = 'COVID19' and r.DIED = 1 and (r.PATIENT_HISTORY like '%crohn%' or r.PATIENT_HISTORY like '%gerd%');

-- guillain barre
select r.VAERS_ID, r.AGE_YEARS, r.VACCINATION_DATE, v.VACCINATION_TYPE, r.DIED, r.DATE_DIED, r.SYMPTOM_TEXT, r.LAB_DATA, r.PATIENT_HISTORY
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%barre%');


-- creutzfeldt jakob disease
select r.VAERS_ID, r.AGE_YEARS, r.VACCINATION_DATE, v.VACCINATION_TYPE, r.DIED, r.DATE_DIED, r.SYMPTOM_TEXT, r.LAB_DATA, r.PATIENT_HISTORY
from reports r left join vaccine v on r.VAERS_ID = v.VAERS_ID
where v.VACCINATION_TYPE = 'COVID19' and (r.SYMPTOM_TEXT like '%creutz%' or r.SYMPTOM_TEXT like '%cjd%');

