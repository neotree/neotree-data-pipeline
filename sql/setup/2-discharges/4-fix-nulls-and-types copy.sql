drop table if exists derived.discharges;
create table derived.discharges as 
select 
    uid,
    ingested_at,
	case when 	"Apgar10DC"	='blank' then null else	"Apgar10DC"::float	end as 	"Apgar10DC",
	case when 	"Apgar1DC"	='blank' then null else	"Apgar1DC"::float	end as 	"Apgar1DC",
	case when 	"Apgar5DC"	='blank' then null else	"Apgar5DC"::float	end as 	"Apgar5DC",
	case when 	"BWDC"	='blank' then null else	"BWDC"::float	end as 	"BWDC",
	case when 	"CadreDis"	='blank' then null else	"CadreDis"	end as 	"CadreDis",
	case when 	"CauseDeath"	='blank' then null else	"CauseDeath"	end as 	"CauseDeath",
	case when 	"CauseDeathOther"	='blank' then null else	"CauseDeathOther"	end as 	"CauseDeathOther",
	case when 	"CLINREVDAT"	='blank' then null else	"CLINREVDAT"	end as 	"CLINREVDAT",
	case when 	"ContCauseDeath"	='blank' then null else	"ContCauseDeath"	end as 	"ContCauseDeath",
	case when 	"ContribOth"	='blank' then null else	"ContribOth"	end as 	"ContribOth",
	case when 	"DateAdmissionDC"	='blank' then null else	"DateAdmissionDC"	end as 	"DateAdmissionDC",
	case when 	"DateDischVitals"	='blank' then null else	"DateDischVitals"	end as 	"DateDischVitals",
	case when 	"DateDischWeight"	='blank' then null else	"DateDischWeight"	end as 	"DateDischWeight",
	case when 	"DateTimeDeath"	='blank' then null else	"DateTimeDeath"	end as 	"DateTimeDeath",
	case when 	"DateTimeDischarge"	='blank' then null else	"DateTimeDischarge"	end as 	"DateTimeDischarge",
	case when 	"DateWeaned"	='blank' then null else	"DateWeaned"	end as 	"DateWeaned",
	case when 	"DIAGDIS1"	='blank' then null else	"DIAGDIS1"	end as 	"DIAGDIS1",
	case when 	"DIAGDIS1OTH"	='blank' then null else	"DIAGDIS1OTH"	end as 	"DIAGDIS1OTH",
	case when 	"DischHR"	='blank' then null else	"DischHR"::float	end as 	"DischHR",
	case when 	"DischRR"	='blank' then null else	"DischRR"::float	end as 	"DischRR",
	case when 	"DischSats"	='blank' then null else	"DischSats"::float	end as 	"DischSats",
	case when 	"DischTemp"	='blank' then null else	"DischTemp"::float	end as 	"DischTemp",
	case when 	"DischWeight"	='blank' then null else	"DischWeight"::float	end as 	"DischWeight",
	case when 	"EndScriptDatetime"	='blank' then null else	"EndScriptDatetime"	end as 	"EndScriptDatetime",
	case when 	"FeedsAdm"	='blank' then null else	"FeedsAdm"	end as 	"FeedsAdm",
	case when 	"GestationDC"	='blank' then null else	"GestationDC"::float	end as 	"GestationDC",
	case when 	"HCWIDDis"	='blank' then null else	"HCWIDDis"	end as 	"HCWIDDis",
	case when 	"HCWSigDis"	='blank' then null else	"HCWSigDis"	end as 	"HCWSigDis",
	case when 	"HealthEd"	='blank' then null else	"HealthEd"::boolean	end as 	"HealthEd",
	case when 	"HIVtestResultDC"	='blank' then null else	"HIVtestResultDC"	end as 	"HIVtestResultDC",
	case when 	"MEDOTH"	='blank' then null else	"MEDOTH"	end as 	"MEDOTH",
	case when 	"MedsGiven"	='blank' then null else	"MedsGiven"	end as 	"MedsGiven",
	case when 	"ModeDeliveryDC"	='blank' then null else	"ModeDeliveryDC"::float	end as 	"ModeDeliveryDC",
	case when 	"ModFactor1"	='blank' then null else	"ModFactor1"	end as 	"ModFactor1",
	case when 	"ModFactor2"	='blank' then null else	"ModFactor2"	end as 	"ModFactor2",
	case when 	"ModFactor3"	='blank' then null else	"ModFactor3"	end as 	"ModFactor3",
	case when 	"NeoTreeID"	='blank' then null else	"NeoTreeID"	end as 	"NeoTreeID",
	case when 	"NeoTreeOutcome"	='blank' then null else	"NeoTreeOutcome"	end as 	"NeoTreeOutcome",
	case when 	"NUID_S"	='blank' then null else	"NUID_S"	end as 	"NUID_S",
	case when 	"OtherProbs"	='blank' then null else	"OtherProbs"	end as 	"OtherProbs",
	case when 	"OtherProbsOth"	='blank' then null else	"OtherProbsOth"	end as 	"OtherProbsOth",
	case when 	"PHOTOTHERAPY"	='blank' then null else	"PHOTOTHERAPY"::boolean	end as 	"PHOTOTHERAPY",
	case when 	"RESPSUP"	='blank' then null else	"RESPSUP"	end as 	"RESPSUP",
	case when 	"REVCLIN"	='blank' then null else	"REVCLIN"	end as 	"REVCLIN",
	case when 	"REVCLINOTH"	='blank' then null else	"REVCLINOTH"	end as 	"REVCLINOTH",
	case when 	"REVCLINTYP"	='blank' then null else	"REVCLINTYP"	end as 	"REVCLINTYP",
	case when 	"ThermCare"	='blank' then null else	"ThermCare"	end as 	"ThermCare",
	case when 	"UIDDC"	='blank' then null else	"UIDDC"	end as 	"UIDDC"

from scratch.discharges_form_exploded;