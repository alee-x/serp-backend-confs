
clear all


***********************************************************
* CLEANING SEED DATA
***********************************************************
insheet using "./data_seed.csv" 

gen Seed_gender = .
replace Seed_gender = 0 if q111m_gender == 0
replace Seed_gender = 1 if q111f_gender == 1
label variable Seed_gender "Seed Gender" 
label define seedgender_l 0 "Male Seeds" 1 "Female Seeds" 
label values Seed_gender seedgender_l
tab Seed_gender
sort serialnumber

gen savings = q219_ifyes_howmuchinr
replace savings = 0 if q218_saving_monthlyinr  == 0 
encode q211_spouseeducationlevel, gen(q211_spouseeducation)
replace q211_spouseeducation = . if q211_spouseeducation == 7
label variable q200_borninthisvillage "Born in village"
label variable q201_birthyear  "Year of birth"
label variable q202_householdmember "Household size"
label variable q205_havechildren "Number of children"
label variable q206_yourchildrengotoschool "Number of children to school"
label variable q209_caste "Caste"
label variable q210_educationlevel "Education" 
label variable q211_spouseeducation "Education of spouse" 
label variable q212_readhindi "Read hindi"
label variable q215_ownbusiness "Owns a business"
label variable q217_householdexpenses_monthly  "Expenses"
label variable savings  "Savings"
label variable q220_currentlyindebtedinr "In debt" 
label variable q304_ownhouse "Owns the house"
label variable q305_owntotallandacre  "Land"
label variable q306_irrigatedlandacre "Irrigated land"
label variable q307_owncattle "Owns cattle"
label variable q311_mobilephone "Has a phone"
label variable q402_numberofkerosenelamps "Number of kerosene lamps"

replace q219_ifyes_howmuchinr = 0 if q219_ifyes_howmuchinr == .
sum q219_ifyes_howmuchinr
hist q219_ifyes_howmuchinr
graph close

* Lighting
replace q406_ifyes_useforstudying = 0 if q405_childrenuselighting ==0
label variable q404_c_cookingcleaninglaundry "If use lighting for household chores"
label variable q404_a_business "If use lighting for business"
label variable q404_b_studying "If use lighting for studying"
label variable q406_ifyes_useforstudying "If children use lighting for studying"
label variable q404_d_meetingwithfriendsandfami "If use lighting to meet friends"
label variable q408_b_kerosene "Monthly spending on kerosene"
* replace the 6th option 'don t know' with missing
replace q409_badforyoureyesight = . if q409_badforyoureyesight==6
replace q410_badforyourhealth = . if q410_badforyourhealth==6
replace q413_sufficientlightingforcookin = . if q413_sufficientlightingforcookin==6
label variable q409_badforyoureyesight "Current lighting bad for eyesight"
label variable q410_badforyourhealth "Kerosene lighting bad for health"
label variable q411_victimofakerosenefire "Was victim of kerosene fire"
label variable q412_victimofakerosenefire  "Knows a victim of a kerosene fire"
label variable q413_sufficientlightingforcookin "Sufficient light for cooking"
label variable q414_safegoingoutsideatnight  "Feel safe going outside"
label variable q415_feelsaferiftherewasmoreligh "Would feel safer if more light"
label variable q416_satisfiedwithlighting "Satisfied with current lighting"
gen q505_mostly_myself =  q505_mostlybeusingthesolarlanter == 1
gen q505_mostly_spouse =  q505_mostlybeusingthesolarlanter == 2
gen q505_mostly_children =  q505_mostlybeusingthesolarlanter == 3
label variable q505_mostly_myself "Mostly be using: myself"
label variable q505_mostly_spouse "Mostly be using: spouse"
label variable q505_mostly_children "Mostly be using: children"
codebook q505_mostlybeusingthesolarlanter q505_mostly_myself q505_mostly_spouse q505_mostly_children
label variable q506_d_meetingwithfriendsandfami "Will use it to meet friends"
label variable q508_solarlanterncostsinr "Lanter cost guess"

label variable q600_a_thelocalhealthcenter "Need permission: health center"
label variable q600_b_thehomeofrelativesorfrien "Need permission: friend's house"
label variable q600_c_thelocalshopormarket "Need permission: local market"
label variable q601_youandyourspousetalkaboutsp "Talk about spendings"
label variable q604_womenshouldworkoutside "Women should work outside/business"
label variable q605_a_ifshegoesoutwithouttellin "Beating justified if goes out without telling "
label variable q605_b_ifsheargueswithherhusband "Beating justified if argues"
label variable q605_c_havingrelationswithotherm "Beating justified if suspected adultery"
replace q703_arebetterabletousenewtechno = . if q703_arebetterabletousenewtechno==6
label variable q703_arebetterabletousenewtechno "Men better at technology"
* social life
label variable q800_a_villagemeetings "Going to village meetings (freq)"
label variable q800_b_farmerscooperativemeeting "Going to coop meetings (freq)"
label variable q800_c_selfhelpgroupevents "Going to self-help group events (freq)"
label variable q800_d_religiousgroupevents "Going to religious events (freq)"
label variable q800_e_politicalpartyevents "Going to politcal events (freq)"
label variable q801_yourvillagecooperative "People are cooperative"
label variable q802_trustothervillagers "Trust other villagers"
label variable q803_spendsparetimewithfriendsor "Spend time with friends"
rename q_805_numberoffriendsinthisvilla q805_numberoffriendsinthisvilla
label variable q805_numberoffriendsinthisvilla "Number of friends"


***********************************************************
* SAVING SEED DATA
saveold seed_data, replace version(12)
***********************************************************


***********************************************************
* CLEANING PEERS DATA
***********************************************************
clear all
insheet using "./data_control.csv" 
generate str24 treat = "CONTROL" 
rename *, lower
saveold sample_data, replace version(12)

clear all
insheet using "./data_network.csv" 
generate str24 treat = "NETWORK" 
rename *, lower
codebook q409_fuelforcooking_otherspecify
tostring q409_fuelforcooking_otherspecify, replace /*string in control. no values and so in byte in coomunication */
codebook q507_ifyes_useforstudying /*is a string but should be a numeric */
destring q507_ifyes_useforstudying, replace
codebook q602a_ifyessowhere_otherspecify /*string in control. no values and so in byte in coomunication must be in string */
tostring q602a_ifyessowhere_otherspecify, replace
codebook q402_maintoiletfacility_otherspe
append using sample_data.dta, force /* have to use force because some variable are string in some sheets and numeric in others eg. Q409_FuelforCooking_OtherSpec */
saveold sample_data, replace version(12)


clear all
insheet using "./data_communication.csv" 
generate str24 treat = "COMMUNICATION" 
rename *, lower
codebook q409_fuelforcooking_otherspecify
tostring q409_fuelforcooking_otherspecify, replace /*string in control. no values and so in byte in coomunication */
tostring q402_maintoiletfacility_otherspe, replace
codebook q217_householdexpenses_monthly
append using sample_data.dta, force /* have to use force because some variable are string in some sheets and numeric in others eg. Q409_FuelforCooking_OtherSpec */
saveold sample_data, replace version(12)



gen treatment = .
replace treatment = 1 if treat == "CONTROL"
replace treatment = 2 if treat == "NETWORK"
replace treatment = 3 if treat == "COMMUNICATION"
tab treatment
label define treatment 1 "Control" 2 "Network" 3 "Communication"
label values treatment treatment
label variable treatment "Treatment group"
drop treat

* Data cleaning
rename q_800_canimprovethequalityofmyho q800_canimprovethequalityofmyho
rename v105 q605_mostlybeusingthesolarlant_o
order treat treatment serialnumber q001_householdid q100_idofinterviewer q103_dateofinterview

* changing the format of the data (it s a string in the raw data)
codebook q103_dateofinterview
gen dateInt = date(q103_dateofinterview, "DMY", 2099)
format dateInt %td
drop q103_dateofinterview
rename  dateInt q103_dateofinterview

* SPOUSE BIRTH - MUST REPLACE "99" WITH MISSING
describe q204_spousesbirth
tab q204_spousesbirth
replace q204_spousesbirth = . if q204_spousesbirth == 99

* EDUCATION SPOUSE - q211 IS A MESS -- CLEAN
describe q211_spouseeducationlevel
tab q211_spouseeducationlevel
encode q211_spouseeducationlevel, generate(q211_new)
describe q211_new
tab q211_new
replace q211_spouseeducationlevel = ""
destring q211_spouseeducationlevel, replace
replace q211_spouseeducationlevel = q211_new
replace q211_spouseeducationlevel = . if q211_spouseeducationlevel == 7
replace q211_spouseeducationlevel = . if q211_spouseeducationlevel == 8
drop q211_new

* SAVINGS: REPLACE MISSING WITH 0 IF q218 IS NOT MISSING
describe q219_ifyes_howmuchinr q218_saving_monthlyinr
tab q219_ifyes_howmuchinr 
tab q218_saving_monthlyinr
replace q219_ifyes_howmuchinr = 0 if q218_saving_monthlyinr == 0 

* SOLAR LANTERN SEEN: REPLACE MISSING WITH 0 IF q602_ever NOT MISSING
describe q602a_ifyessowhere
tab q602a_ifyessowhere
tab q602_seenasolarlanternbefore
replace q602a_ifyessowhere = 0 if q602_seenasolarlanternbefore == 0

*CONVERSATION: REPLACE q604 MISSING WITH 0 IF q603 NOT MISSING
describe q604_conversationwiththispers
tab q604_conversationwiththispers
tab q603_knowsomeonewithasolarl
replace q604_conversationwiththispers = 0 if q603_knowsomeonewithasolarl == 0

* 3 network and 3 communication missing for the whole questionnaire  
* Dropping the 3 network and 3 comm that are missing. 
sort serialnumber
drop if serialnumber == 28
drop if serialnumber == 103
drop if serialnumber == 127

* WTP is also missing for a few
* q302_WTPsol q303_thepriceyouhavedrawnissolar 
* 6 missing
* serial number 66 network
* serial number 80 network
* serial number 102 communication
* serial number 107 network
* serial number 174 communication
* serial number 197 communication
* a total of 9 habitations for which we don't have WTP

saveold sample_data, replace version(12)



**************************************
* Add Info about the seeds         
**************************************
clear all
use seed_data
rename q303_mainsourceofdrinkingwater_o q303_mainsourceofdrinkwat_o
foreach v of var * {
	local new = substr("`v'", 1, 30)
	rename `v' s_`new'
}
rename s_Seed_gender Seed_gender
rename s_serialnumber serialnumber

sort serialnumber
merge 1:m serialnumber using sample_data
sort serialnumber treatment
saveold sample_data, replace version(12)



**************************************
* Creating useful variables           
**************************************
gen control = treatment == 1
gen network = treatment == 2
gen communication = treatment == 3
gen network_gender = network*Seed_gender
gen comm_gender = communication*Seed_gender
label variable control "Control"
label variable network "Unincentivized"
label variable communication "Incentivized"
label variable Seed_gender "Female Seed"
label variable network_gender "Unincentivized x Female Seed"
label variable comm_gender "Incentivized x Female Seed"


* Preparing variables and labels
gen Seed_Treat = .
replace Seed_Treat = 100 if treatment == 1 & Seed_gender == 0
replace Seed_Treat = 101 if treatment == 1 & Seed_gender == 1
replace Seed_Treat = 200 if treatment == 2 & Seed_gender == 0
replace Seed_Treat = 201 if treatment == 2 & Seed_gender == 1
replace Seed_Treat = 300 if treatment == 3 & Seed_gender == 0
replace Seed_Treat = 301 if treatment == 3 & Seed_gender == 1
label define seedtreat 100 "Control M" 101 "Control F" 200 "Unincen M" 201 "Unincen F" 300 "Incenti M" 301 "Incenti F"
label values Seed_Treat seedtreat
label variable Seed_Treat "Treatment and Seed group" 

* WTP: 
rename q302_yourfinalbidissolarlan q302_WTPsol
label variable q302_WTPsol "WTP"
gen WTP_male_seed = q302_WTPsol if Seed_gender == 0
gen WTP_female_seed = q302_WTPsol if Seed_gender == 1
label variable WTP_male_seed "WTP - Male Seed"
label variable WTP_female_seed "WTP - Female Seed"
gen WTP_control = q302_WTPsol if treatment == 1
gen WTP_network = q302_WTPsol if treatment == 2
gen WTP_commmunication = q302_WTPsol if treatment == 3
label variable WTP_control "WTP - Control"
label variable WTP_network "WTP - Unincentivized"
label variable WTP_commmunication "WTP - Incentivized"

* risk profile
gen risk_aversion = .
replace risk_aversion = 5 if q1001 == "A"
replace risk_aversion = 4 if q1003 == "A"
replace risk_aversion = 3 if q1003 == "B"
replace risk_aversion = 2 if q1004 == "A"
replace risk_aversion = 1 if q1004 == "B"
codebook risk_aversion
tab risk_aversion treatment
label variable risk_aversion "Risk Aversion" 


**************************************
* Labels                              
**************************************
label variable Seed_gender "Seed Gender"
label variable q103_dateofinterview "Interview date"
label variable q302_WTPsol "WTP"
label variable q111_gender "If respondent is female"
label variable q201_birthyear  "Year of birth"
label variable q205_havechildren "Number of children"
label variable q210_educationlevel "Education" 
rename q217_householdexpenses_monthly q217_expensesmonth 
label variable q217_expensesmonth  "Monthly Expenses"
label variable q219_ifyes_howmuchinr  "Amount of Savings"
label variable q215_ownbusiness "If owns a business"
label variable q202_householdmember "Household size"
label variable q206_yourchildrengotoschool "Number of children to school"
label variable q212_readhindi "If reads hindi"
label variable q220_currentlyindebtedinr "If in debt"
label variable q404_ownhouse "If owns the house"
label variable q405_owntotallandacre  "Land (acres)"
label variable q406_irrigatedlandacre "Irrigated land (acres)"
label variable q407_owncattle "If owns cattle"
label variable q411_mobilephone "If has a phone"
label variable q503_numberofkerosenelamps "Number of kerosene lamps"
label variable q508_hours_uselighting "Hours of lighting"
label define gender 0 "Male" 1 "Female" 
label values q111_gender gender
label define seedgender_f 0 "Male Seed Friends" 1 "Female Seed Friends" 
label values Seed_gender seedgender_f

**************************************
* Exploring Other Variables                              
**************************************
* create variable that equals 0 if respondent does not know a person who owns a lantern, 1 if knows a person and interacts with her once year, =2 for once a month, =3 for once a week , = 4 for more than 3 times a week
gen interaction_withowner = .
replace interaction_withowner = 0 if  q604_conversationwiththisperson == 0
replace interaction_withowner = 1 if  q604_conversationwiththisperson == 4
replace interaction_withowner = 2 if  q604_conversationwiththisperson == 3
replace interaction_withowner = 3 if  q604_conversationwiththisperson == 2
replace interaction_withowner = 4 if  q604_conversationwiththisperson == 1

* replace the 6th option 'don t know' with missing
replace q609_solarlanternisaninnovativep = . if q609_solarlanternisaninnovativep==6
replace q610_solarlanternisasuperiorthan = . if q610_solarlanternisasuperiorthan==6
replace q611_useasolarlanterninsteadofak = . if q611_useasolarlanterninsteadofak==6
replace q510_badforyoureyesight = . if q510_badforyoureyesight==6
replace q511_badforyourhealth = . if q511_badforyourhealth==6
replace q514_sufficientlightingforcookin = . if q514_sufficientlightingforcookin==6
replace q515_safegoingoutsideatnight = . if q515_safegoingoutsideatnight==6
replace q516_feelsaferiftherewasmoreligh = . if q516_feelsaferiftherewasmoreligh==6
replace q517_satisfiedwithlighting = . if q517_satisfiedwithlighting==6

* create dummy variables
gen q605_mostly_myself =  q605_mostlybeusingthesolarlanter == 1
gen q605_mostly_spouse =  q605_mostlybeusingthesolarlanter == 2
gen q605_mostly_children =  q605_mostlybeusingthesolarlanter == 3
label variable q605_mostly_myself "Mostly be using: myself"
label variable q605_mostly_spouse "Mostly be using: spouse"
label variable q605_mostly_children "Mostly be using: children"

* Other variables
label variable q209_caste "Caste"
label variable q505_c_cookingcleaninglaundry "If use lighting for household chores"
label variable q505_a_business "If use lighting for business"
label variable q505_b_studying "If use lighting for studying"
label variable q507_ifyes_useforstudying "If children use lighting for studying"
label variable q505_d_meetingwithfriendsandfami "If use lighting to meet friends"
label variable q509_b_kerosene "Monthly spending on kerosene"
label variable q509_c_batterychargedlamps "Monthly spending on batteries"
label variable q510_badforyoureyesight "Current lighting bad for eyesight"
label variable q511_badforyourhealth "Kerosene lighting bad for health"
label variable q512_victimofakerosenefire "Was victim of kerosene fire"
label variable q513_victimofakerosenefire  "Knows a victim of a kerosene fire"
label variable q514_sufficientlightingforcookin "Sufficient light for cooking"
label variable q515_safegoingoutsideatnight  "Feel safe going outside"
label variable q516_feelsaferiftherewasmoreligh "Would feel safer if more light"
label variable q517_satisfiedwithlighting "Satisfied with current lighting"
label variable q704_womenshouldworkoutside "Women should work outside/business"
label variable q705_a_ifshegoesoutwithouttellin "Beating justified if goes out without telling "
label variable q705_b_ifsheargueswithherhusband "Beating justified if argues"
label variable q705_c_havingrelationswithotherm "Beating justified if suspected adultery"
label variable q900_a_villagemeetings "Going to village meetings (freq)"
label variable q900_b_farmerscooperativemeeting "Going to coop meetings (freq)"
label variable q900_c_selfhelpgroupevents "Going to self-help group events (freq)"
label variable q900_d_religiousgroupevents "Going to religious events (freq)"
label variable q900_e_politicalpartyevents "Going to politcal events (freq)"
label variable q901_yourvillagecooperative "People are cooperative"
label variable q902_trustothervillagers "Trust other villagers"
label variable q903_spendsparetimewithfriendsor "Spend time with friends"
label variable q905_numberoffriendsinthisvillag "Number of friends"



label variable q602_seenasolarlanternbefore "If has seen a solar lantern before"
label variable q603_knowsomeonewithasolarlanter "Knows someone with a solar lantern"

label variable q607_solarlanterncanfunctionprop "Believes can function without maintenance"
gen q607_maintenance = .
replace q607_maintenance = 0 if q607_solarlanterncanfunctionprop == 1
replace q607_maintenance = 1 if q607_solarlanterncanfunctionprop == 0
label variable q607_maintenance "Believes maintenance is important"


label variable q608_solarlanterncostsinr "Estimated cost of the solar lantern"
label variable q609_solarlanternisaninnovativep "Believes it is innovative"
label variable q610_solarlanternisasuperiorthan "Believes it is superior to kerosene lamps"
label variable q611_useasolarlanterninsteadofak "Would recommend over ker. lamp"


label variable q700_a_thelocalhealthcenter "1. Should ask permission to go the health center"
label variable q700_b_thehomeofrelativesorfrien "2. Should ask permission to go visit a friend"
label variable q700_c_thelocalshopormarket "3. Should ask permission to go to the market"
label variable q701_youandyourspousetalkaboutsp "4. Talk about what to spend money on with spouse"
label variable q702_womanshouldsayonhouseholdsp "5. Women should have a say on how to spend income"
label variable q703_importantthatgirlsgotoschoo "6. It is importnat that girls go to school"
label variable q704_womenshouldworkoutsid "7. Women should work outside home or own a business"
label variable q705_a_ifshegoesoutwithouttellin "8. Beating justified if she goes out without telling"
label variable q705_b_ifsheargueswithherhusband "9. Beating justified if she argues with husband"
label variable q705_c_havingrelationswithotherm "10. Beating justified if suspected of adultery"
label variable q803_arebetterabletousenewtechno "11. Men are better able to use new technologies than women"

gen spending_month = q509_b_kerosene + q509_c_batterychargedlamps
sum spending_month q503_numberofkerosenelamps
gen spending_month_perlamp =  spending_month / q503_numberofkerosenelamps
sum spending_month_perlamp
gen spending_yr_perlamp =  spending_month_perlamp * 12
sum spending_yr_perlamp
label variable spending_month_perlamp "Monthly spending per lamp"
label variable spending_month "Spending on kerosene (mth)"


gen purchased = 1 if q302_WTPsol >= q303_thepriceyouhavedrawnissolar
replace purchased = 0 if q302_WTPsol < q303_thepriceyouhavedrawnissolar
tab purchased treatment


saveold sample_data, replace version(12)







