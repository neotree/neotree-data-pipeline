DROP TABLE IF EXISTS derived.summary_joined_admissions_discharges;
CREATE TABLE derived.summary_joined_admissions_discharges AS
SELECT "source"."uid" AS "uid", "source"."AdmissionDateTime" AS "AdmissionDateTime", "source"."Readmitted" AS "Readmitted", "source"."admission_source" AS "admission_source", "source"."referredFrom" AS "referredFrom", "source"."Gender.label" AS "Gender", "source"."AdmissionWeight" AS "AdmissionWeight", "source"."AdmissionWeightGroup" AS "AdmissionWeightGroup", "source"."BirthWeight" AS "BirthWeight", "source"."BirthWeightGroup" AS "BirthWeightGroup", "source"."Gestation" AS "Gestation", "source"."ModeOfEsttimating" AS "ModeOfEsttimating", "source"."AgeCategory" AS "AgeCategory", "source"."MotherHIVTest" AS "MotherHIVTest", "source"."HIVTestResult" AS "HIVTestResult", "source"."OnHAART" AS "OnHAART", "source"."LengthOfHAART" AS "LengthOfHAART", "source"."NVPgiven" AS "NVPgiven", "source"."TempGroup" AS "TempGroup", "source"."Temperature" AS "Temperature", "source"."GestationGroup" AS "GestationGroup", "source"."InOrOut" AS "InOrOut", "source"."FacilityReferredFrom" AS "FacilityReferredFrom", "source"."DischargeDateTime" AS "DischargeDateTime", "source"."NeonateOutcome" AS "NeonateOutcome", "source"."AdmissionMonthYear" AS "AdmissionMonthYear", "source"."AdmissionMonthYearSort" AS "AdmissionMonthYearSort", "source"."AntenatalSteroids" AS "AntenatalSteroids", "source"."Less28wks/1kgCount", "source"."PretermCount","source"."DeathCount" AS "DeathCount", "source"."DischargeCount" AS "DischargeCount", "source"."BirthWeightCount" AS "BirthWeightCount", "source"."AdmissionWeightCount" AS "AdmissionWeightCount", "source"."GestationCount" AS "GestationCount", "source"."OutsideFacilityCount" AS "OutsideFacilityCount", "source"."WithinFacilityCount" AS "WithinFacilityCount", "source"."AdmissionCount" AS "AdmissionCount", "source"."PrematureCount" As "PrematureCount", "source"."HypothermiaCount" as "HypothermiaCount",
"source"."TempThermiaSort" AS "TempThermiaSort", "source"."TempThermia" AS "TempThermia", "source"."BirthWeightSort" AS "BirthWeightSort", "source"."AdmissionWeightSort" As "AdmissionWeightSort", "source"."GestSort" AS "GestSort", "source"."AgeCatSort" as "AgeCatSort",
"source"."AbscondedCount" AS "AbscondedCount", "source"."TransferredOutCount" as "TransferredOutCount", "source"."DischargeOnRequestCount" AS "DischargeOnRequestCount", "source"."Death<24hrsCount" As "DeathLessThan24hrs", "source"."Death>24hrsCount" AS "DeathMoreThan24hrs", "source"."NNDCount" AS "NNDCount"
FROM (SELECT "derived"."joined_admissions_discharges"."uid" AS "uid", 
		"derived"."joined_admissions_discharges"."DateTimeAdmission.value" as "AdmissionDateTime",
		"derived"."joined_admissions_discharges"."Readmission.label" as "Readmitted", 
		"derived"."joined_admissions_discharges"."AdmittedFrom.label" as "admission_source",
        "derived"."joined_admissions_discharges"."ReferredFrom2.label" as "referredFrom", 
        "derived"."joined_admissions_discharges"."Gender", 
        "derived"."joined_admissions_discharges"."AW.value" as "AdmissionWeight", 
        "derived"."joined_admissions_discharges"."AWGroup.value" as "AdmissionWeightGroup", 
        "derived"."joined_admissions_discharges"."BW.value" as "BirthWeight", 
        "derived"."joined_admissions_discharges"."BWGroup.value" as "BirthWeightGroup",
        "derived"."joined_admissions_discharges"."Genitalia.value" as "Gestation", 
        "derived"."joined_admissions_discharges"."MethodEstGest.label" as "ModeOfEsttimating", 
        "derived"."joined_admissions_discharges"."AgeCat.label" as "AgeCategory",
        "derived"."joined_admissions_discharges"."MatHIVtest.label" as "MotherHIVTest", 
        "derived"."joined_admissions_discharges"."HIVtestResult.label" as "HIVTestResult", 
        "derived"."joined_admissions_discharges"."HAART.label" as "OnHAART",
        "derived"."joined_admissions_discharges"."LengthHAART.label" as "LengthOfHAART",
        "derived"."joined_admissions_discharges"."TempThermia.value" as "TempThermia",
        "derived"."joined_admissions_discharges"."NVPgiven.label" as "NVPgiven", 
        "derived"."joined_admissions_discharges"."TempGroup.value" as "TempGroup",
        "derived"."joined_admissions_discharges"."Temperature.value" as "Temperature", 
        "derived"."joined_admissions_discharges"."GestGroup.value" As "GestationGroup",
        "derived"."joined_admissions_discharges"."InOrOut.label" as "InOrOut",
        "derived"."joined_admissions_discharges"."ReferredFrom.label" AS "FacilityReferredFrom",
        "derived"."joined_admissions_discharges"."DateTimeDischarge.value" AS "DischargeDateTime",
      	"derived"."joined_admissions_discharges"."NeoTreeOutcome.label" AS "NeonateOutcome", 
        CAST(TO_CHAR(DATE("derived"."joined_admissions_discharges"."DateTimeAdmission.value") :: DATE, 'Mon-YYYY') AS text) AS "AdmissionMonthYear", 
        CAST(TO_CHAR(DATE("derived"."joined_admissions_discharges"."DateTimeAdmission.value") :: DATE, 'YYYYmm') AS decimal) AS "AdmissionMonthYearSort",
        "derived"."joined_admissions_discharges"."ANSteroids.label" As "AntenatalSteroids",
        CASE WHEN "derived"."joined_admissions_discharges"."Gestation.value" < 28 AND "derived"."joined_admissions_discharges"."BW.value" < 1000 then 1 End as "Less28wks/1kgCount",
        CASE WHEN "derived"."joined_admissions_discharges"."GestGroup.value" <> 'Term' THEN 1 END AS "PretermCount",
      	Case 
         when "derived"."joined_admissions_discharges"."NeoTreeOutcome.label" like '%%Death%%' THEN 1 
         when "derived"."joined_admissions_discharges"."NeoTreeOutcome.label" like '%%NND%%' THEN 1 
       	end as "DeathCount",
       	Case when "derived"."joined_admissions_discharges"."NeoTreeOutcome.label" = 'Absconded' Then 1 end as "AbscondedCount",
       	Case when "derived"."joined_admissions_discharges"."NeoTreeOutcome.label" = 'Transferred to other hospital' Then 1 end as "TransferredOutCount",
       	Case when "derived"."joined_admissions_discharges"."NeoTreeOutcome.label" = 'Discharged on Request' Then 1 end as "DischargeOnRequestCount",
       	Case when "derived"."joined_admissions_discharges"."NeoTreeOutcome.label" IN ('Death (at LESS than 24 hrs of age)', 'NND less than 24 hrs old' ) Then 1 end as "Death<24hrsCount",
        Case when "derived"."joined_admissions_discharges"."NeoTreeOutcome.label" IN ('Death (at MORE than 24 hrs of age)', 'NND more than 24 hrs old' ) Then 1 end as "Death>24hrsCount",
        Case when "derived"."joined_admissions_discharges"."NeoTreeOutcome.label" = 'NND' Then 1 end as "NNDCount",
       	Case when "derived"."joined_admissions_discharges"."NeoTreeOutcome.label" IS NOT NULL THEN 1 end as "DischargeCount",
       	Case when "derived"."joined_admissions_discharges"."BWGroup.value" IS NOT NULL THEN 1 end as "BirthWeightCount",
       	Case when "derived"."joined_admissions_discharges"."AWGroup.value" IS NOT NULL THEN 1 end as "AdmissionWeightCount",
       	Case when "derived"."joined_admissions_discharges"."GestGroup.value" IS NOT NULL THEN 1 end as "GestationCount",
       	Case when "derived"."joined_admissions_discharges"."InOrOut.label" like '%%Outside%%' THEN 1 end as "OutsideFacilityCount",
		Case when "derived"."joined_admissions_discharges"."InOrOut.label" like '%%Within%%' THEN 1  end as "WithinFacilityCount",
		Case when "derived"."joined_admissions_discharges"."DateTimeAdmission.value" IS NOT NULL Then 1  end as "AdmissionCount",
		CASE when "derived"."joined_admissions_discharges"."BW.value" < 2500 THEN 1 end as "PrematureCount",
		CASE when "derived"."joined_admissions_discharges"."TempThermia.value" = 'Hypothermia' Then 1 end as "HypothermiaCount",
		CASE when "derived"."joined_admissions_discharges"."TempThermia.value" = 'Hypothermia' Then 1
		     when "derived"."joined_admissions_discharges"."TempThermia.value" = 'Normothermia' Then 2
		     when "derived"."joined_admissions_discharges"."TempThermia.value" = 'Hyperthermia' Then 3
		End as "TempThermiaSort",
		CASE 
		    when "derived"."joined_admissions_discharges"."AWGroup.value" = '<1000g' THEN 1
		    when "derived"."joined_admissions_discharges"."AWGroup.value" = '1000-1500g' THEN 2
		    when "derived"."joined_admissions_discharges"."AWGroup.value" = '1500-2500g' THEN 3
		    when "derived"."joined_admissions_discharges"."AWGroup.value" = '2500-4000g' THEN 4
	        when "derived"."joined_admissions_discharges"."AWGroup.value" = '>4000g' THEN 5
        END as "AdmissionWeightSort",
        CASE 
		    when "derived"."joined_admissions_discharges"."BWGroup.value" = 'Unknown' THEN 6
		    when "derived"."joined_admissions_discharges"."BWGroup.value" = 'ELBW' THEN 1
		    when "derived"."joined_admissions_discharges"."BWGroup.value" = 'VLBW' THEN 2
		    when "derived"."joined_admissions_discharges"."BWGroup.value" = 'LBW' THEN 3
	        when "derived"."joined_admissions_discharges"."BWGroup.value" = 'NBW' THEN 4
	        when "derived"."joined_admissions_discharges"."BWGroup.value" = 'HBW' THEN 5
        END as "BirthWeightSort",
        CASE
            when "derived"."joined_admissions_discharges"."GestGroup.value" = '<28' THEN 1
            when "derived"."joined_admissions_discharges"."GestGroup.value" = '28-32 wks' THEN 2
            when "derived"."joined_admissions_discharges"."GestGroup.value" = '32-34 wks' THEN 3
            when "derived"."joined_admissions_discharges"."GestGroup.value" = '34-36+6 wks' THEN 4
            when "derived"."joined_admissions_discharges"."GestGroup.value" = 'Term' THEN 5
        END as "GestSort",
        CASE
            when "derived"."joined_admissions_discharges"."AgeCat.label" = 'Fresh Newborn (< 2 hours old)' THEN 1
            when "derived"."joined_admissions_discharges"."AgeCat.label" = 'Newborn (2 - 23 hrs old)' THEN 2
            when "derived"."joined_admissions_discharges"."AgeCat.label" = 'Newborn (1 day - 1 day 23 hrs old)' THEN 3
            when "derived"."joined_admissions_discharges"."AgeCat.label" = 'Infant (2 days - 2 days 23 hrs old)' THEN 4
            when "derived"."joined_admissions_discharges"."AgeCat.label" = 'Infant (> 3 days old)' THEN 5
        END as "AgeCatSort"
FROM "derived"."joined_admissions_discharges"
ORDER BY "derived"."joined_admissions_discharges"."uid" ASC
) "source"










