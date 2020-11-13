-- Fix birth weights

update derived.admissions set "AW.value" = 1640 WHERE "uid" ='F55F-0513';  -- original value was 164
update derived.admissions set "AW.value" = 2000 WHERE "uid" ='6367-0975';  -- original value was 200
update derived.admissions set "AW.value" = 2350 WHERE "uid" ='F55F-0118';  -- original value was 235
update derived.admissions set "AW.value" = 3000 WHERE "uid" ='0BC7-0292';  -- original value was 3
update derived.admissions set "AW.value" = 3000 WHERE "uid" ='B385-0321';  -- original value was 3
update derived.admissions set "AW.value" = 3000 WHERE "uid" ='F55F-0665';  -- original value was 3
update derived.admissions set "AW.value" = 3000 WHERE "uid" ='F55F-0815';  -- original value was 3
update derived.admissions set "AW.value" = 3020 WHERE "uid" ='0BC7-0324';  -- original value was 302
update derived.admissions set "AW.value" = 3300 WHERE "uid" ='9525-0817';  -- original value was 33
update derived.admissions set "AW.value" = 4000 WHERE "uid" ='B385-0196';  -- original value was 1
update derived.admissions set "AW.value" = 4000 WHERE "uid" ='6367-0862';  -- original value was 4
update derived.admissions set "AW.value" = 4200 WHERE "uid" ='A7C6-0350';  -- original value was 42
update derived.admissions set "AW.value" = 4200 WHERE "uid" ='A7C6-0378';  -- original value was 42

-- Fix admission weights

update derived.admissions set "BW.value" =1000 WHERE uid='A7C6-0022'; -- Original value was 100
update derived.admissions set "BW.value" =1000 WHERE uid='6367-1109'; -- Original value was 100
update derived.admissions set "BW.value" =1385 WHERE uid='F55F-0343'; -- Original value was 385
update derived.admissions set "BW.value" =1400 WHERE uid='6367-0898'; -- Original value was 14
update derived.admissions set "BW.value" =1700 WHERE uid='A46C-0206'; -- Original value was 170
update derived.admissions set "BW.value" =2000 WHERE uid='B385-0330'; -- Original value was 200
update derived.admissions set "BW.value" =2000 WHERE uid='A46C-0214'; -- Original value was 2
update derived.admissions set "BW.value" =2350 WHERE uid='F55F-0118'; -- Original value was 
update derived.admissions set "BW.value" =2500 WHERE uid='F55F-0805'; -- Original value was 250
update derived.admissions set "BW.value" =3000 WHERE uid='0BC7-0292'; -- Original value was 
update derived.admissions set "BW.value" =3000 WHERE uid='F55F-0815'; -- Original value was 
update derived.admissions set "BW.value" =3000 WHERE uid='F55F-0820'; -- Original value was 300
update derived.admissions set "BW.value" =3050 WHERE uid='B385-0218'; -- Original value was 350
update derived.admissions set "BW.value" =3100 WHERE uid='F55F-0785'; -- Original value was 31
update derived.admissions set "BW.value" =3180 WHERE uid='C22B-0117'; -- Original value was 0
update derived.admissions set "BW.value" =3600 WHERE uid='F55F-0467'; -- Original value was 36000
update derived.admissions set "BW.value" =3800 WHERE uid='A7C6-0350'; -- Original value was 38
update derived.admissions set "BW.value" =3800 WHERE uid='A7C6-0378'; -- Original value was 38
