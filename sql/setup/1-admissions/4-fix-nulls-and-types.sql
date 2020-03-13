drop table if exists derived.admissions;
create table derived.admissions as 
select 
    uid,
    ingested_at,

	case when 	"Abdomen"	='blank' then null else	"Abdomen"	end as 	"Abdomen",
	case when 	"Activity"	='blank' then null else	"Activity"	end as 	"Activity",
	case when 	"AdmittedFrom"	='blank' then null else	"AdmittedFrom"	end as 	"AdmittedFrom",
	case when 	"AdmReason"	='blank' then null else	"AdmReason"	end as 	"AdmReason",
	case when 	"AdmReasonOth"	='blank' then null else	"AdmReasonOth"	end as 	"AdmReasonOth",
	case when 	"AgeA"	='blank' then null else	"AgeA"	end as 	"AgeA",
	case when 	"AgeB"	='blank' then null else	"AgeB"	end as 	"AgeB",
	case when 	"AgeC"	='blank' then null else	"AgeC"::float	end as 	"AgeC",
	case when 	"AgeCat"	='blank' then null else	"AgeCat"	end as 	"AgeCat",
	case when 	"ANMatSyphTreat"	='blank' then null else	"ANMatSyphTreat"	end as 	"ANMatSyphTreat",
	case when 	"ANSteroids"	='blank' then null else	"ANSteroids"	end as 	"ANSteroids",
	case when 	"AntenatalCare"	='blank' then null else	"AntenatalCare"	end as 	"AntenatalCare",
	case when 	"Anus2"	='blank' then null else	"Anus2"	end as 	"Anus2",
	case when 	"ANVDRL"	='blank' then null else	"ANVDRL"	end as 	"ANVDRL",
	case when 	"ANVDRLDate"	='blank' then null else	"ANVDRLDate"	end as 	"ANVDRLDate",
	case when 	"ANVDRLResult"	='blank' then null else	"ANVDRLResult"	end as 	"ANVDRLResult",
	case when 	"Apgar1"	='blank' then null else	"Apgar1"::float	end as 	"Apgar1",
	case when 	"Apgar10"	='blank' then null else	"Apgar10"::float	end as 	"Apgar10",
	case when 	"Apgar5"	='blank' then null else	"Apgar5"::float	end as 	"Apgar5",
	case when 	"AW"	='blank' then null else	"AW"::float	end as 	"AW",
	case when 	"BabyCryTriage"	='blank' then null else	"BabyCryTriage"::boolean	end as 	"BabyCryTriage",
	case when 	"BirthFacility"	='blank' then null else	"BirthFacility"	end as 	"BirthFacility",
	case when 	"BirthPlaceSame"	='blank' then null else	"BirthPlaceSame"::boolean	end as 	"BirthPlaceSame",
	case when 	"BrProbs"	='blank' then null else	"BrProbs"::boolean	end as 	"BrProbs",
	case when 	"BSmg"	='blank' then null else	"BSmg"::float	end as 	"BSmg",
	case when 	"BSmmol"	='blank' then null else	"BSmmol"	end as 	"BSmmol",
	case when 	"BW"	='blank' then null else	"BW"::float	end as 	"BW",
	case when 	"Cadre"	='blank' then null else	"Cadre"	end as 	"Cadre",
	case when 	"ChestAusc"	='blank' then null else	"ChestAusc"	end as 	"ChestAusc",
	case when 	"Chlor"	='blank' then null else	"Chlor"	end as 	"Chlor",
	case when 	"Colour"	='blank' then null else	"Colour"	end as 	"Colour",
	case when 	"CRT"	='blank' then null else	"CRT"	end as 	"CRT",
	case when 	"CryBirth"	='blank' then null else	"CryBirth"	end as 	"CryBirth",
	case when 	"DangerSigns"	='blank' then null else	"DangerSigns"	end as 	"DangerSigns",
	case when 	"DangerSigns2"	='blank' then null else	"DangerSigns2"	end as 	"DangerSigns2",
	case when 	"DateHIVtest"	='blank' then null else	"DateHIVtest"	end as 	"DateHIVtest",
	case when 	"DateTimeAdmission"	='blank' then null else	"DateTimeAdmission"	end as 	"DateTimeAdmission",
	case when 	"DateVDRLSameHIV"	='blank' then null else	"DateVDRLSameHIV"	end as 	"DateVDRLSameHIV",
	case when 	"Diagnoses"	='blank' then null else	"Diagnoses"	end as 	"Diagnoses",
	case when 	"DiagnosesOth"	='blank' then null else	"DiagnosesOth"	end as 	"DiagnosesOth",
	case when 	"DurationLab"	='blank' then null else	"DurationLab"::float	end as 	"DurationLab",
	case when 	"Dysmorphic"	='blank' then null else	"Dysmorphic"::boolean	end as 	"Dysmorphic",
	case when 	"EndScriptDatetime"	='blank' then null else	"EndScriptDatetime"	end as 	"EndScriptDatetime",
	case when 	"Ethnicity"	='blank' then null else	"Ethnicity"	end as 	"Ethnicity",
	case when 	"EthnicityOther"	='blank' then null else	"EthnicityOther"	end as 	"EthnicityOther",
	case when 	"FeedingReview"	='blank' then null else	"FeedingReview"	end as 	"FeedingReview",
	case when 	"FeFo"	='blank' then null else	"FeFo"	end as 	"FeFo",
	case when 	"Femorals"	='blank' then null else	"Femorals"	end as 	"Femorals",
	case when 	"FitsTh"	='blank' then null else	"FitsTh"	end as 	"FitsTh",
	case when 	"Fontanelle"	='blank' then null else	"Fontanelle"	end as 	"Fontanelle",
	case when 	"FontTh"	='blank' then null else	"FontTh"	end as 	"FontTh",
	case when 	"FurtherTriage"	='blank' then null else	"FurtherTriage"	end as 	"FurtherTriage",
	case when 	"Gender"	='blank' then null else	"Gender"	end as 	"Gender",
	case when 	"Genitalia"	='blank' then null else	"Genitalia"	end as 	"Genitalia",
	case when 	"Gestation"	='blank' then null else	"Gestation"::float	end as 	"Gestation",
	case when 	"GraspTh"	='blank' then null else	"GraspTh"::float	end as 	"GraspTh",
	case when 	"GSCvsOM"	='blank' then null else	"GSCvsOM"	end as 	"GSCvsOM",
	case when 	"HAART"	='blank' then null else	"HAART"	end as 	"HAART",
	case when 	"HCWID"	='blank' then null else	"HCWID"	end as 	"HCWID",
	case when 	"HCWSig"	='blank' then null else	"HCWSig"	end as 	"HCWSig",
	case when 	"HeadShape"	='blank' then null else	"HeadShape"	end as 	"HeadShape",
	case when 	"HIVtestResult"	='blank' then null else	"HIVtestResult"	end as 	"HIVtestResult",
	case when 	"HR"	='blank' then null else	"HR"::float	end as 	"HR",
	case when 	"HypoSxYN"	='blank' then null else	"HypoSxYN"	end as 	"HypoSxYN",
	case when 	"InOrOut"	='blank' then null else	"InOrOut"::boolean	end as 	"InOrOut",
	case when 	"IPT"	='blank' then null else	"IPT"	end as 	"IPT",
	case when 	"LengthHAART"	='blank' then null else	"LengthHAART"	end as 	"LengthHAART",
	case when 	"LengthResus"	='blank' then null else	"LengthResus"::float	end as 	"LengthResus",
	case when 	"LengthResusKnown"	='blank' then null else	"LengthResusKnown"	end as 	"LengthResusKnown",
	case when 	"LOCTh"	='blank' then null else	"LOCTh"::float	end as 	"LOCTh",
	case when 	"ManualHR"	='blank' then null else	"ManualHR"	end as 	"ManualHR",
	case when 	"MaritalStat"	='blank' then null else	"MaritalStat"	end as 	"MaritalStat",
	case when 	"MatAgeYrs"	='blank' then null else	"MatAgeYrs"::float	end as 	"MatAgeYrs",
	case when 	"MatHIVtest"	='blank' then null else	"MatHIVtest"::boolean	end as 	"MatHIVtest",
	case when 	"MatPhysAddressDistrict"	='blank' then null else	"MatPhysAddressDistrict"	end as 	"MatPhysAddressDistrict",
	case when 	"MecPresent"	='blank' then null else	"MecPresent"	end as 	"MecPresent",
	case when 	"MecThickThin"	='blank' then null else	"MecThickThin"	end as 	"MecThickThin",
	case when 	"MethodEstGest"	='blank' then null else	"MethodEstGest"	end as 	"MethodEstGest",
	case when 	"ModeDelivery"	='blank' then null else	"ModeDelivery"::float	end as 	"ModeDelivery",
	case when 	"MoroTh"	='blank' then null else	"MoroTh"::float	end as 	"MoroTh",
	case when 	"MSKproblems"	='blank' then null else	"MSKproblems"	end as 	"MSKproblems",
	case when 	"Murmur"	='blank' then null else	"Murmur"	end as 	"Murmur",
	case when 	"NUID_S"	='blank' then null else	"NUID_S"	end as 	"NUID_S",
	case when 	"NVPgiven"	='blank' then null else	"NVPgiven"::boolean	end as 	"NVPgiven",
	case when 	"OFC"	='blank' then null else	"OFC"::float	end as 	"OFC",
	case when 	"OtherBirthFacility"	='blank' then null else	"OtherBirthFacility"	end as 	"OtherBirthFacility",
	case when 	"OtherReferralFacility"	='blank' then null else	"OtherReferralFacility"	end as 	"OtherReferralFacility",
	case when 	"Palate"	='blank' then null else	"Palate"	end as 	"Palate",
	case when 	"PassedMec"	='blank' then null else	"PassedMec"	end as 	"PassedMec",
	case when 	"PlaceBirth"	='blank' then null else	"PlaceBirth"	end as 	"PlaceBirth",
	case when 	"Plan"	='blank' then null else	"Plan"	end as 	"Plan",
	case when 	"PlanOth"	='blank' then null else	"PlanOth"	end as 	"PlanOth",
	case when 	"PostTh"	='blank' then null else	"PostTh"::float	end as 	"PostTh",
	case when 	"PregConditions"	='blank' then null else	"PregConditions"	end as 	"PregConditions",
	case when 	"Presentation"	='blank' then null else	"Presentation"	end as 	"Presentation",
	case when 	"ProbsLab"	='blank' then null else	"ProbsLab"	end as 	"ProbsLab",
	case when 	"PUInfant"	='blank' then null else	"PUInfant"	end as 	"PUInfant",
	case when 	"PUNewborn"	='blank' then null else	"PUNewborn"	end as 	"PUNewborn",
	case when 	"Readmission"	='blank' then null else	"Readmission"	end as 	"Readmission",
	case when 	"Reason"	='blank' then null else	"Reason"	end as 	"Reason",
	case when 	"ReasonOther"	='blank' then null else	"ReasonOther"	end as 	"ReasonOther",
	case when 	"ReferredFrom"	='blank' then null else	"ReferredFrom"	end as 	"ReferredFrom",
	case when 	"ReferredFrom2"	='blank' then null else	"ReferredFrom2"	end as 	"ReferredFrom2",
	case when 	"Religion"	='blank' then null else	"Religion"	end as 	"Religion",
	case when 	"ReligionOther"	='blank' then null else	"ReligionOther"	end as 	"ReligionOther",
	case when 	"RespSR"	='blank' then null else	"RespSR"	end as 	"RespSR",
	case when 	"RespTh"	='blank' then null else	"RespTh"	end as 	"RespTh",
	case when 	"Resus"	='blank' then null else	"Resus"	end as 	"Resus",
	case when 	"RFSepsis"	='blank' then null else	"RFSepsis"	end as 	"RFSepsis",
	case when 	"ROM"	='blank' then null else	"ROM"	end as 	"ROM",
	case when 	"ROMLength"	='blank' then null else	"ROMLength"	end as 	"ROMLength",
	case when 	"RR"	='blank' then null else	"RR"::float	end as 	"RR",
	case when 	"SatsAir"	='blank' then null else	"SatsAir"::float	end as 	"SatsAir",
	case when 	"SatsO2"	='blank' then null else	"SatsO2"::float	end as 	"SatsO2",
	case when 	"SignsDehydrations"	='blank' then null else	"SignsDehydrations"	end as 	"SignsDehydrations",
	case when 	"SignsRD"	='blank' then null else	"SignsRD"	end as 	"SignsRD",
	case when 	"Skin"	='blank' then null else	"Skin"	end as 	"Skin",
	case when 	"Spine"	='blank' then null else	"Spine"	end as 	"Spine",
	case when 	"SRNeuroOther"	='blank' then null else	"SRNeuroOther"	end as 	"SRNeuroOther",
	case when 	"Stethoscope"	='blank' then null else	"Stethoscope"::boolean	end as 	"Stethoscope",
	case when 	"StoolsInfant"	='blank' then null else	"StoolsInfant"	end as 	"StoolsInfant",
	case when 	"SuckReflex"	='blank' then null else	"SuckReflex"	end as 	"SuckReflex",
	case when 	"SuckTh"	='blank' then null else	"SuckTh"	end as 	"SuckTh",
	case when 	"Temperature"	='blank' then null else	"Temperature"::float	end as 	"Temperature",
	case when 	"TestThisPreg"	='blank' then null else	"TestThisPreg"	end as 	"TestThisPreg",
	case when 	"TetraEye"	='blank' then null else	"TetraEye"	end as 	"TetraEye",
	case when 	"ThompScore"	='blank' then null else	"ThompScore"	end as 	"ThompScore",
	case when 	"Tone"	='blank' then null else	"Tone"	end as 	"Tone",
	case when 	"ToneTh"	='blank' then null else	"ToneTh"	end as 	"ToneTh",
	case when 	"Tribe"	='blank' then null else	"Tribe"	end as 	"Tribe",
	case when 	"TribeOther"	='blank' then null else	"TribeOther"	end as 	"TribeOther",
	case when 	"TTV"	='blank' then null else	"TTV"	end as 	"TTV",
	case when 	"TypeBirth"	='blank' then null else	"TypeBirth"	end as 	"TypeBirth",
	case when 	"UID"	='blank' then null else	"UID"	end as 	"UID",
	case when 	"Umbilicus"	='blank' then null else	"Umbilicus"	end as 	"Umbilicus",
	case when 	"VitK"	='blank' then null else	"VitK"	end as 	"VitK",
	case when 	"Vomiting"	='blank' then null else	"Vomiting"	end as 	"Vomiting",
	case when 	"WOB"	='blank' then null else	"WOB"	end as 	"WOB"

from scratch.admissions_form_exploded;