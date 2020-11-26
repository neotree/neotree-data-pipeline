DROP TABLE IF EXISTS derived.summary_joined_admissions_discharges;

CREATE TABLE derived.summary_joined_admissions_discharges AS
SELECT "derived"."joined_admissions_discharges"."uid" AS "uid", 
		"derived"."joined_admissions_discharges"."DateTimeAdmission.value" as "AdmissionDateTime",
		"derived"."joined_admissions_discharges"."Readmission.label" as "Readmitted", 
		"derived"."joined_admissions_discharges"."AdmittedFrom.label" as "admission_source",
        "derived"."joined_admissions_discharges"."ReferredFrom2.label" as "referredFrom", 
        "derived"."joined_admissions_discharges"."Gender.label" as "Gender", 
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
      	Case 
         when "derived"."joined_admissions_discharges"."NeoTreeOutcome.label" like '%Death%' THEN 1 
         when "derived"."joined_admissions_discharges"."NeoTreeOutcome.label" like '%NND%' THEN 1 
       	end as "DeathCount",
       	Case when "derived"."joined_admissions_discharges"."NeoTreeOutcome.label" IS NOT NULL THEN 1 end as "DischargeCount",
       	Case when "derived"."joined_admissions_discharges"."BWGroup.value" IS NOT NULL THEN 1 end as "BirthWeightCount",
       	Case when "derived"."joined_admissions_discharges"."AWGroup.value" IS NOT NULL THEN 1 end as "AdmissionWeightCount",
       	Case when "derived"."joined_admissions_discharges"."GestGroup.value" IS NOT NULL THEN 1 end as "GestationCount",
       	Case when "derived"."joined_admissions_discharges"."InOrOut.label" like '%Outside%' THEN 1 end as "OutsideFacilityCount",
		Case when "derived"."joined_admissions_discharges"."InOrOut.label" like '%Within%' THEN 1  end as "WithinFacilityCount",
		Case when "derived"."joined_admissions_discharges"."DateTimeAdmission.value" IS NOT NULL Then 1  end as "AdmissionCount"
FROM "derived"."joined_admissions_discharges"
ORDER BY "derived"."joined_admissions_discharges"."uid" ASC;