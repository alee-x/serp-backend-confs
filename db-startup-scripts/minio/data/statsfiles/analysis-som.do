

* Stata version 15
* 18 January 2021

clear all
use ./sample_data


*******************************************************
* TABLES AND FIGURES IN THE SOM
*******************************************************


*******************************************************
*******************************************************
* Baseline summary statistics and test of balance for covariates (long - many variables)
// ssc install balancetable
gen treatment_A = (treatment==1)
gen treatment_B = (treatment==2)
gen age = 2015 - q201_birthyear
label variable age "Age"

balancetable (mean if treatment == 1) (mean if treatment == 2) (mean if treatment == 3) ///
(diff treatment_A if treatment != 3) (diff treatment_A if treatment != 2) (diff treatment_B if treatment != 1) ///
q111_gender age q205_havechildren q210_educationlevel q217_expensesmonth q219_ifyes_howmuchinr ///
q215_ownbusiness q202_householdmember  q206_yourchildrengotoschool q212_readhindi  q220_currentlyindebtedinr  ///
q405_owntotallandacre  q407_owncattle  q411_mobilephone q503_numberofkerosenelamps q508_hours_uselighting spending_month_perlamp ///
using "./Tables/balancetable_long.tex", ///
replace varlabels noobs booktabs  format(%9.2f)  ///
ctitles("Control" "Unincentivized" "Incentivized" "Control vs Unincen." "Control vs Incent." "Unincen. vs Incent.")

* Test of joint equality in means for all three treatment groups
global balancevar q111_gender age q205_havechildren q210_educationlevel q217_expensesmonth q219_ifyes_howmuchinr ///
q215_ownbusiness q202_householdmember  q206_yourchildrengotoschool q212_readhindi  q220_currentlyindebtedinr  ///
q405_owntotallandacre  q407_owncattle  q411_mobilephone q503_numberofkerosenelamps q508_hours_uselighting spending_month_perlamp
foreach x of global balancevar { 
display "`x'"
* Testing whether mean group 1 = mena group 2 = mean group 3 
oneway `x' treatment 
* Testing whether mean group 1 = mena group 2 = mean group 3 - non parametric, in case variables are not normally distributed
kwallis `x', by(treatment)
}
  
* Rank sum tests for variable
global balancevar q111_gender age q205_havechildren q210_educationlevel q217_expensesmonth q219_ifyes_howmuchinr ///
q215_ownbusiness q202_householdmember  q206_yourchildrengotoschool q212_readhindi  q220_currentlyindebtedinr  ///
q405_owntotallandacre  q407_owncattle  q411_mobilephone q503_numberofkerosenelamps q508_hours_uselighting spending_month_perlamp
foreach x of global balancevar { 
display "`x'"
* Control vs Network
ranksum `x' if treatment != 3, by(treatment)
* Control vs Communication
ranksum `x' if treatment != 2, by(treatment)
* Network vs Communication
ranksum `x' if treatment != 1, by(treatment)
}

tab treatment Seed_gender if q302_WTPsol != .

 


*******************************************************
*******************************************************
* Baseline summary statistics and test of balance for variables across treatments and seed gender (long - many variables)
balancetable ///
(mean if treatment == 1 & Seed_gender == 0) ///
(mean if treatment == 1 & Seed_gender == 1) /// 
(diff Seed_gender if treatment == 1) ///
(mean if treatment == 2 & Seed_gender == 0) /// 
(mean if treatment == 2 & Seed_gender == 1) /// 
(diff Seed_gender if treatment == 2) /// 
(mean if treatment == 3 & Seed_gender == 0) ///
(mean if treatment == 3 & Seed_gender == 1) ///
(diff Seed_gender if treatment == 3) ///
q111_gender age q205_havechildren q210_educationlevel q217_expensesmonth q219_ifyes_howmuchinr ///
q215_ownbusiness q202_householdmember  q206_yourchildrengotoschool q212_readhindi  q220_currentlyindebtedinr  ///
q405_owntotallandacre  q407_owncattle  q411_mobilephone q503_numberofkerosenelamps q508_hours_uselighting spending_month_perlamp ///
using "./Tables/balancetable_treatandgenderseed_long.tex", ///
replace varlabels noobs booktabs  format(%9.2f)  nonumbers  ///
groups("Control" "Unincentivized" "Incentivized", pattern(1 0 0 1 0 0 1 0 0)) ///
ctitles("Male" "Female" "Male vs Female" "Male" "Female" "Male vs Female" "Male" "Female" "Male vs Female") 


* Rank sum tests for variable
global balance2 ///
q111_gender age q205_havechildren q210_educationlevel q217_expensesmonth q219_ifyes_howmuchinr ///
q215_ownbusiness q202_householdmember  q206_yourchildrengotoschool q212_readhindi  q220_currentlyindebtedinr  ///
q405_owntotallandacre  q407_owncattle  q411_mobilephone q503_numberofkerosenelamps q508_hours_uselighting spending_month_perlamp 
foreach x of global balance2 { 
display "`x'"
* Control  M vs Control F
ranksum `x'  if treatment ==1, by(Seed_gender)
* Network M vs Network F
ranksum `x' if treatment ==2, by(Seed_gender)
* Communication M vs Commu F
ranksum `x' if treatment ==3, by(Seed_gender)
}
 






*******************************************************
*******************************************************
* NETWORK ANALYSIS: are female choosing "different" friends than male?
* Summary statistics on the characteristics of the friends chosen by male seeds and female seeds
eststo clear
quietly estpost tabstat q111_gender q201_birthyear q205_havechildren q210_educationlevel q217_expensesmonth q219_ifyes_howmuchinr ///
q215_ownbusiness q202_householdmember  q206_yourchildrengotoschool q212_readhindi  q220_currentlyindebtedinr   ///
q405_owntotallandacre  q406_irrigatedlandacre q407_owncattle  q411_mobilephone q503_numberofkerosenelamps q508_hours_uselighting ///
, by(Seed_gender) statistics(mean sd ) columns(statistics) nototal 
esttab using "./Tables/summstats_network.tex", ///
main(mean) aux(sd) nostar unstack  noobs nonote label replace  nomtitles nonumbers compress


* table w t tests
eststo clear
quietly estpost ttest q111_gender q201_birthyear q205_havechildren q210_educationlevel q217_expensesmonth q219_ifyes_howmuchinr ///
q215_ownbusiness q202_householdmember  q206_yourchildrengotoschool q212_readhindi  q220_currentlyindebtedinr   ///
q405_owntotallandacre  q406_irrigatedlandacre q407_owncattle  q411_mobilephone q503_numberofkerosenelamps q508_hours_uselighting ///
,  by(Seed_gender)
esttab using "./Tables/ttest_summstats_network.tex", ///
 replace star(* 0.10 ** 0.05 *** 0.01)    label nonumber compress noobs unstack  mtitles("Difference Male Seed Friends - Female Seed Friends"  )

ranksum q111_gender, by(Seed_gender)
ranksum q201_birthyear, by(Seed_gender)
ranksum q205_havechildren, by(Seed_gender)
ranksum q210_educationlevel, by(Seed_gender)
ranksum q217_expensesmonth, by(Seed_gender)
ranksum q219_ifyes_howmuchinr, by(Seed_gender)
ranksum q215_ownbusiness, by(Seed_gender)
ranksum q202_householdmember, by(Seed_gender)
ranksum q206_yourchildrengotoschool, by(Seed_gender)
ranksum q212_readhindi, by(Seed_gender)
ranksum q220_currentlyindebtedinr, by(Seed_gender)
ranksum q405_owntotallandacre, by(Seed_gender)
ranksum q406_irrigatedlandacre, by(Seed_gender)
ranksum q407_owncattle, by(Seed_gender)
ranksum q411_mobilephone, by(Seed_gender)
ranksum q503_numberofkerosenelamps, by(Seed_gender)
ranksum q508_hours_uselighting, by(Seed_gender)

tabstat q111_gender , by(Seed_gender)




*******************************************************
*******************************************************
* HETEROGENEITY - SUMMARY STATS: FEMALE VS MALE    
* Summary statistics by gender of the respondent
eststo clear
quietly estpost tabstat q201_birthyear q205_havechildren q210_educationlevel q217_expensesmonth q219_ifyes_howmuchinr ///
q215_ownbusiness q202_householdmember  q206_yourchildrengotoschool q212_readhindi  q220_currentlyindebtedinr   ///
q405_owntotallandacre  q406_irrigatedlandacre q407_owncattle  q411_mobilephone q503_numberofkerosenelamps q508_hours_uselighting ///
, by(q111_gender) statistics(mean sd ) columns(statistics) nototal 
esttab using "./Tables/summstats_genderrespondent.tex", ///
main(mean) aux(sd) nostar unstack  noobs nonote label replace  nomtitles nonumbers compress

* table w t tests
eststo clear
quietly estpost ttest q201_birthyear q205_havechildren q210_educationlevel q217_expensesmonth q219_ifyes_howmuchinr ///
q215_ownbusiness q202_householdmember  q206_yourchildrengotoschool q212_readhindi  q220_currentlyindebtedinr   ///
q405_owntotallandacre  q406_irrigatedlandacre q407_owncattle  q411_mobilephone q503_numberofkerosenelamps q508_hours_uselighting ///
,  by(q111_gender)
esttab using "./Tables/ttest_summstats_genderrespondent.tex", ///
 replace star(* 0.10 ** 0.05 *** 0.01)    label nonumber compress noobs unstack  mtitles("Difference Male - Female"  )




*******************************************************
*******************************************************
* Means and t-tests across treatments.
label variable q302_WTPsol "WTP - All seeds"

* Table combining means and t-tests
balancetable (mean if treatment == 1) (mean if treatment == 2) (mean if treatment == 3) ///
(diff treatment_A if treatment != 3) (diff treatment_A if treatment != 2) (diff treatment_B if treatment != 1) ///
q302_WTPsol  WTP_male_seed WTP_female_seed  ///
using "./Tables/means_combinedtable.tex", ///
replace varlabels noobs booktabs  format(%9.2f)  ///
ctitles("Control" "Unincentivized" "Incentivized" "Control vs Unincen." "Control vs Incent." "Unincen. vs Incent.")
* Then add manually one line for the tests for diff between male and female 

* Table with means across treatments
eststo clear
quietly estpost tabstat q302_WTPsol WTP_male_seed WTP_female_seed , by(treatment) statistics(mean sd ) columns(statistics) nototal   
esttab using "./Tables/means.tex",  ///
main(mean) aux(sd) unstack  noobs nonote label replace  nomtitles nonumbers compress  addnotes("Variable: Willingness to Pay. Means and standard deviations.")

*  t-tests for differences across treatments
eststo clear
eststo: quietly estpost ttest q302_WTPsol WTP_male_seed WTP_female_seed if treatment != 3, by(treatment)
eststo: quietly estpost ttest q302_WTPsol WTP_male_seed WTP_female_seed if treatment != 2, by(treatment)
eststo: quietly estpost ttest q302_WTPsol WTP_male_seed WTP_female_seed if treatment != 1, by(treatment)
esttab using "./Tables/test_means_WTP_treatments.tex", ///
replace star(* 0.10 ** 0.05 *** 0.01)  mtitles("Control - Unincen." "Control - Incenti." "Unincen. - Incenti.") label nonumber compress noobs unstack ///
mgroups("" "Differences", pattern(1 1 0) span) 

* Rank sum tests across treatments: * If variable is normal --> ttest command
* Wilcoxon-Mann-Whitney test for variables that are not normal --> ranksum command
global mainoutcomes q302_WTPsol WTP_male_seed WTP_female_seed
foreach x of global mainoutcomes { 
display "`x'"
* Control vs Network
ranksum `x' if treatment != 3, by(treatment)
* Control vs Communication
ranksum `x' if treatment != 2, by(treatment)
* Network vs Communication
ranksum `x' if treatment != 1, by(treatment)
}

* t tests across gender within each treatment
eststo clear
eststo: quietly estpost ttest WTP_control WTP_network WTP_commmunication, by(Seed_gender)
esttab using "./Tables/test_means_WTP_gender.tex", ///
replace star(* 0.10 ** 0.05 *** 0.01)  mtitles("Male - Female" )  label nonumber compress noobs unstack ///
mgroups("Difference", pattern(1) span) 
label variable q302_WTPsol "WTP"

* Rank sum tests across gender within each treatment
global mainoutcomes WTP_control WTP_network WTP_commmunication
foreach x of global mainoutcomes { 
display "`x'"
* male seed vs female seed in control	
ranksum WTP_control, by(Seed_gender)
* male seed vs female seed in network	
ranksum WTP_network, by(Seed_gender)
* male seed vs female seed in communication	
ranksum WTP_commmunication, by(Seed_gender)
}
 
* Comparing the 3 means at the same time
* p-value = 0.002  = probability that the observed association between the 2 variables has occurred by chance
* Ho : WTP is not associated with the treatment group
estpost tabulate q302_WTPsol treatment, chi2
*Pearson chi2(34) =  94.1772   Pr = 0.000
estpost tabulate WTP_male_seed treatment, chi2
*Pearson chi2(30) =  60.9900   Pr = 0.001
estpost tabulate WTP_female_seed treatment, chi2
*Pearson chi2(32) =  55.6925   Pr = 0.006



*******************************************************
label variable q302_WTPsol "WTP"
gen network_gender_femalehead = network_gender * q111_gender
gen comm_gender_femalehead = comm_gender * q111_gender
label variable network_gender_femalehead "Unincentivized x Female Seed x Female Head"
label variable comm_gender_femalehead "Incentivized x Female Seed x Female Head"
gen gender_femalehead = Seed_gender * q111_gender
label variable gender_femalehead "Female Seed x Female Head"
codebook q111_gender
gen same_gender = 0
replace same_gender = 1 if q111_gender == 1 & Seed_gender == 1
replace same_gender = 1 if q111_gender == 0 & Seed_gender == 0
tab treatment same_gender
label variable same_gender "Same Gender"
gen network_same_gender = network * same_gender
gen comm_same_gender = communication * same_gender
label variable network_same_gender "Unincentivized x Same Gender"
label variable comm_same_gender "Incentivized x Same Gender"
gen control_gender = control*Seed_gender
label variable control_gender "Control x Female Seed"
gen control_same_gender = control * same_gender
label variable control_same_gender "Control x Same Gender"



*******************************************************
*******************************************************
* PROGRAM FOR REGRESSIONS
capture program drop IncenVsUninc
* this program outputs the coeff and se of Incentivized vs Unincentivized treatment
* this will be added to the main table as the last line
program define IncenVsUninc, rclass
				xtset serialnumber 
				xtreg `1', fe vce(cluster serialnumber)
				local coef1 `=string(_b[communication], "%9.2f")' 
				matrix tablecoef = r(table)
				* matrix list tablecoef
				local pvalue tablecoef[4, 2]
				display `pvalue'

				if `pvalue' < 0.01 {
				local coef1 "`coef1'***"
				}

				else if `pvalue' < 0.05 {
				local coef1 "`coef1'**"
				}
				else if `pvalue' < 0.1 {
				local coef1 "`coef1'*"
				}

				display "`coef1'"
				local coef2 `=string(_se[communication], "%9.2f")'
				local coef2 "(`coef2')"
				display "`coef2'"
				local coef2p `=string(tablecoef[4, 2], "%9.2f")'
				local coef2p "[`coef2p']"
				display "`coef2p'"
				
				local coef3 `=string(_b[comm_gender], "%9.2f")' 
				display "`coef3'"
				local coef4 `=string(_se[comm_gender], "%9.2f")'
				local coef4 "(`coef4')"
				display "`coef4'"
				local coef4p `=string(tablecoef[4, 4], "%9.2f")'
				local coef4p "[`coef4p']"
				display "`coef4p'"

				local coef5 `=string(_b[comm_same_gender], "%9.2f")' 
				display "`coef5'"
				local coef6 `=string(_se[comm_same_gender], "%9.2f")'
				local coef6 "(`coef6')"
				display "`coef6'"
				local coef6p `=string(tablecoef[4, 4], "%9.2f")'
				local coef6p "[`coef6p']"
				display "`coef6p'"

				return local coef1 `"`coef1'"'
				return local coef2 `"`coef2'"'		
				return local coef2p `"`coef2p'"'		
				return local coef3 `"`coef3'"'
				return local coef4 `"`coef4'"'	
				return local coef4p `"`coef4p'"'	
				return local coef5 `"`coef5'"'
				return local coef6 `"`coef6'"'	
				return local coef6p `"`coef6p'"'	
end





*******************************************************
*******************************************************
* ROBUSTNESS DROPPING HABITATION WHERE THERE ALREADY WAS A LANTERN
* Treatment effects on WTP dropping habitations with solar lantern owners
* Construct variable that says whether respondent already had a lantern
list serialnumber if q600_ownasolarlantern == 1
gen hassolarlantern = 0
replace hassolarlantern = 1 if serialnumber == 20
replace hassolarlantern = 1 if serialnumber == 54
replace hassolarlantern = 1 if serialnumber == 56
replace hassolarlantern = 1 if serialnumber == 85
replace hassolarlantern = 1 if serialnumber == 100
replace hassolarlantern = 1 if serialnumber == 191
replace hassolarlantern = 1 if serialnumber == 196
replace hassolarlantern = 1 if serialnumber == 199

* Construct variable that says whether, in the habitation, the control bought the lantern
gen got_lantern = . 
replace got_lantern = 1 if q302_WTPsol >= q303_thepriceyouhavedrawnissolar
replace got_lantern = 0 if q302_WTPsol < q303_thepriceyouhavedrawnissolar
tab got_lantern treatment
gen got_lantern_control = 0 
replace got_lantern_control = 1 if got_lantern == 1 & treatment == 1
* tab got_lantern_control treatment
* tab got_lantern_control
egen doubleseed = sum(got_lantern_control), by(serialnumber)
label variable doubleseed "Control Seed"
gen doubleseed_network = network * doubleseed
gen doubleseed_communication = communication * doubleseed
label variable doubleseed_network "Unincentivized x Control Seed"
label variable doubleseed_communication "Incentivized x Control Seed"


xtset serialnumber 
eststo clear

IncenVsUninc "q302_WTPsol control communication  "
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q302_WTPsol network communication,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local fixedeffects "Yes" , replace

IncenVsUninc "q302_WTPsol control communication if hassolarlantern == 0"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q302_WTPsol network communication if hassolarlantern == 0,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local fixedeffects "Yes" , replace
 

IncenVsUninc "q302_WTPsol control communication if doubleseed == 0"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q302_WTPsol network communication if doubleseed == 0,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local fixedeffects "Yes" , replace

IncenVsUninc "q302_WTPsol control communication if doubleseed == 0 & hassolarlantern == 0"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q302_WTPsol network communication if doubleseed == 0 & hassolarlantern == 0,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local fixedeffects "Yes" , replace


// esttab using "./Tables/robustnessresults_nolantern.tex", ///
// replace b(%9.2f) stats(Compcoeff Compse  fixedeffects r2 N, fmt(%9.2f  %9.2f  %~12s  %9.2f %9.3g) ///
// label("Incentivized vs. Unincentivized" " " "Habitation fixed effects"  "R-squared" "Observations")) booktabs eqlabels(none) ///
// constant se label star(* 0.10 ** 0.05 *** 0.01) ///
// mtitles("All" "No Lantern Before" "No Control Purchase" "Both")  noisily notype


 
#delimit ;
estout using `"./Tables/robustnessresults_nolantern.tex"' ,
 cells(b(fmt(%9.2f) star) se(fmt(%9.2f) par))
 stats(Compcoeff Compse  fixedeffects r2 N, fmt(%9.2f  %9.2f  %~12s  %9.2f %9.3g) label("Incentivized vs. Unincentivized" " ""Habitation fixed effects"   "R-squared" "Observations"))
 starlevels(`"\sym{*}"' 0.10 `"\sym{**}"' 0.05 `"\sym{***}"' 0.01, label(" \(p<@\)"))
 varwidth(20)
 modelwidth(12)
 delimiter(&)
 end(\\)
 prehead(`"{"' `"\def\sym#1{\ifmmode^{#1}\else\(^{#1}\)\fi}"' `"\begin{tabular}{l*{@E}{c}}"' `"\toprule"')
 posthead("\midrule")
 prefoot("\midrule")
 postfoot(`"\bottomrule"' `"\multicolumn{@span}{l}{\footnotesize Standard errors clustered at the habitation level are in parentheses.}\\"' `"\multicolumn{@span}{l}{\footnotesize @starlegend} \\"' `"\end{tabular}"' `"}"')
 label
 varlabels(_cons Constant, end("" \addlinespace) nolast)
 mlabels("All" "No Lantern Before" "No Control Purchase" "Both", titles span prefix(\multicolumn{@span}{c}{) suffix(}))
 numbers(\multicolumn{@span}{c}{( )})
 collabels(none)
 eqlabels(none)
 substitute(_ \_ "\_cons " \_cons)
 interaction(" $\times$ ")
 notype
 level(95)
 style(esttab)
 replace ;
#delimit cr





*******************************************************
*******************************************************
* Heterogeneity -  Seed savings
* Treatment effects on WTP and Seed Savings.
rename s_q217_householdexpenses_monthly s_q217_expensesmonth
codebook s_q217_expensesmonth s_q219_ifyes_howmuchinr
gen net_savings = network * s_q219_ifyes_howmuchinr
label variable net_savings "Unincentivized x Seed Savings"
gen com_savings = communication * s_q219_ifyes_howmuchinr
label variable com_savings "Incentivized x Seed Savings"
label variable s_q219_ifyes_howmuchinr "Seed Savings"
label variable s_q217_expensesmonth "Seed Expenses"
gen net_expen = network * s_q217_expensesmonth
label variable net_expen "Unincentivized x Seed Expenses"
gen com_expen = communication * s_q217_expensesmonth
label variable com_expen "Incentivized x Seed Expenses"


xtset serialnumber 
eststo clear

IncenVsUninc "q302_WTPsol control communication  "
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q302_WTPsol network communication,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local fixedeffects "Yes" , replace

IncenVsUninc "q302_WTPsol control communication  "
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q302_WTPsol network communication,  ///
vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local fixedeffects "No" , replace

IncenVsUninc "q302_WTPsol control communication s_q219_ifyes_howmuchinr net_savings com_savings"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q302_WTPsol network communication s_q219_ifyes_howmuchinr ///
net_savings com_savings,  ///
vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local fixedeffects "No" , replace

esttab using "./Tables/main_results_hetero_seedsavings.tex", ///
replace b(%9.2f) stats(fixedeffects secluster r2_w N, fmt(%~12s %~12s %9.2f %9.3g) ///
label("Habitation fixed effects" "Clustered SE (habitation)" "R-squared" "Observations")) booktabs eqlabels(none) ///
noconstant se label star(* 0.10 ** 0.05 *** 0.01) ///
mtitles("WTP" "WTP" "WTP" ) noisily notype


#delimit ;
estout using `"./Tables/main_results_hetero_seedsavings.tex"' ,
 cells(b(fmt(%9.2f) star) se(fmt(%9.2f) par))
 stats(Compcoeff Compse  fixedeffects r2_w N, fmt(%9.2f  %9.2f  %~12s  %9.2f %9.3g) label("Incentivized vs. Unincentivized" " ""Habitation fixed effects"   "R-squared" "Observations"))
 starlevels(`"\sym{*}"' 0.10 `"\sym{**}"' 0.05 `"\sym{***}"' 0.01, label(" \(p<@\)"))
 varwidth(20)
 modelwidth(12)
 delimiter(&)
 end(\\)
 prehead(`"{"' `"\def\sym#1{\ifmmode^{#1}\else\(^{#1}\)\fi}"' `"\begin{tabular}{l*{@E}{c}}"' `"\toprule"')
 posthead("\midrule")
 prefoot("\midrule")
 postfoot(`"\bottomrule"' `"\multicolumn{@span}{l}{\footnotesize Standard errors clustered at the habitation level are in parentheses.}\\"' `"\multicolumn{@span}{l}{\footnotesize @starlegend} \\"' `"\end{tabular}"' `"}"')
 label
 varlabels(_cons Constant, end("" \addlinespace) nolast)
 mlabels("WTP" "WTP" "WTP" , titles span prefix(\multicolumn{@span}{c}{) suffix(}))
 numbers(\multicolumn{@span}{c}{( )})
 collabels(none)
 eqlabels(none)
 substitute(_ \_ "\_cons " \_cons)
 interaction(" $\times$ ")
 notype
 level(95)
 style(esttab)
 replace ;
#delimit cr






*******************************************************
*******************************************************
* Summary statistics for some key solar lantern related variables highlighting possible mechanisms
balancetable (mean if treatment == 1) (mean if treatment == 2) (mean if treatment == 3) ///
(diff treatment_A if treatment != 3) (diff treatment_A if treatment != 2) (diff treatment_B if treatment != 1) ///
q602_seenasolarlanternbefore q603_knowsomeonewithasolarlanter ///
q607_maintenance q608_solarlanterncostsinr ///
q609_solarlanternisaninnovativep q610_solarlanternisasuperiorthan q611_useasolarlanterninsteadofak ///
using "./Tables/balancetable_mechanism.tex", ///
replace varlabels noobs booktabs  format(%9.2f)  ///
ctitles("Control" "Unincentivized" "Incentivized" "Control vs Unincen." "Control vs Incent." "Unincen. vs Incent.")






*******************************************************
*******************************************************
* LASSO
* Treatment effects on WTP controlling for more covariates through the LASSO algorithm

xtset serialnumber 
eststo clear

IncenVsUninc "q302_WTPsol control communication q111_gender q219_ifyes_howmuchinr q210_educationlevel q217_expensesmonth q220_currentlyindebtedinr q202_householdmember q206_yourchildrengotoschool q503_numberofkerosenelamps q508_hours_uselighting  spending_month"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q302_WTPsol network communication /// 
q111_gender q219_ifyes_howmuchinr q210_educationlevel q217_expensesmonth q220_currentlyindebtedinr ///
q202_householdmember q206_yourchildrengotoschool q503_numberofkerosenelamps q508_hours_uselighting  spending_month,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace

quietly rlasso q302_WTPsol control communication /// 
q111_gender q219_ifyes_howmuchinr q210_educationlevel q217_expensesmonth q220_currentlyindebtedinr ///
q202_householdmember q206_yourchildrengotoschool q503_numberofkerosenelamps   ///
q508_hours_uselighting  spending_month, fe pnotpen(control communication) 
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly rlasso q302_WTPsol network communication /// 
q111_gender q219_ifyes_howmuchinr q210_educationlevel q217_expensesmonth q220_currentlyindebtedinr ///
q202_householdmember q206_yourchildrengotoschool q503_numberofkerosenelamps   ///
q508_hours_uselighting  spending_month, fe pnotpen(network communication) 
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "  " , replace
estadd local fixedeffects "Yes" , replace

IncenVsUninc "q302_WTPsol control communication "
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q302_WTPsol network communication,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace

esttab using "./Tables/robustness_controlsLASSO.tex", ///
replace b(%9.2f) stats(Compcoeff Compse fixedeffects r2 N, fmt(%9.2f %9.2f %~12s %~12s %9.2f %9.3g) ///
label("Incentivized vs. Unincentivized" " " "Habitation fixed effects"  "R-squared" "Observations")) booktabs eqlabels(none) ///
constant se label star(* 0.10 ** 0.05 *** 0.01) ///
mtitles("OLS" "LASSO" "OLS")  noisily notype

#delimit ;
estout using `"./Tables/robustness_controlsLASSO.tex"' ,
 cells(b(fmt(%9.2f) star) se(fmt(%9.2f) par))
 stats(Compcoeff Compse fixedeffects r2 N, fmt(%9.2f %9.2f %~12s %~12s %9.2f %9.3g) label("Incentivized vs. Unincentivized" " " "Habitation fixed effect s"  "R-squared" "Observations"))
 starlevels(`"\sym{*}"' 0.10 `"\sym{**}"' 0.05 `"\sym{***}"' 0.01, label(" \(p<@\)"))
 varwidth(20)
 modelwidth(12)
 delimiter(&)
 end(\\)
 prehead(`"{"' `"\def\sym#1{\ifmmode^{#1}\else\(^{#1}\)\fi}"' `"\begin{tabular}{l*{@E}{c}}"' `"\toprule"')
 posthead("\midrule")
 prefoot("\midrule")
 postfoot(`"\bottomrule"' `"\multicolumn{@span}{l}{\footnotesize Standard errors in parentheses}\\"' `"\multicolumn{@span}{l}{\footnotesize @starlegend} \\"' `"\end{tabular}"' `"}"')
 label
 varlabels(_cons Constant, end("" \addlinespace) nolast)
 mlabels("OLS" "LASSO" "OLS", titles span prefix(\multicolumn{@span}{c}{) suffix(}))
 numbers(\multicolumn{@span}{c}{( )})
 collabels(none)
 eqlabels(none)
 substitute(_ \_ "\_cons " \_cons)
 interaction(" $\times$ ")
 notype
 level(95)
 style(esttab)
 replace;
#delimit cr





********************************************************************
* ROBUSTNESS DROPPING HABITATION WTP is missing
gen habitationmissingWTP = 0
replace habitationmissingWTP = 1 if serialnumber == 28
replace habitationmissingWTP = 1 if serialnumber == 66
replace habitationmissingWTP = 1 if serialnumber == 80
replace habitationmissingWTP = 1 if serialnumber == 102
replace habitationmissingWTP = 1 if serialnumber == 103
replace habitationmissingWTP = 1 if serialnumber == 107
replace habitationmissingWTP = 1 if serialnumber == 127
replace habitationmissingWTP = 1 if serialnumber == 174
replace habitationmissingWTP = 1 if serialnumber == 197

xtset serialnumber 
eststo clear
eststo: quietly xtreg q302_WTPsol network communication if habitationmissingWTP == 0,  ///
fe vce(cluster serialnumber)
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace

eststo: quietly xtreg q302_WTPsol network communication network_gender comm_gender if habitationmissingWTP == 0,  ///
fe vce(cluster serialnumber)
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace

eststo: quietly xtreg q302_WTPsol network communication network_gender comm_gender ///
if q111_gender == 0 & habitationmissingWTP == 0,  ///
fe vce(cluster serialnumber)
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace


esttab using "./Tables/main_results_drophabmissingWTP.tex", ///
 replace b(%9.2f) stats(fixedeffects secluster r2 N, fmt(%~12s %~12s %9.2f %9.3g) ///
label("Habitation fixed effects" "Clustered SE (habitation)" "R-squared" "Observations")) booktabs eqlabels(none) ///
noconstant se label star(* 0.10 ** 0.05 *** 0.01) ///
mtitles("WTP" "WTP" "Male head only") noisily notype

#delimit ;
estout using `"./Tables/main_results_drophabmissingWTP.tex"' ,
 cells(b(fmt(%9.2f) star) se(fmt(%9.2f) par))
 drop(_cons, relax)
 stats(fixedeffects  r2 N, fmt(%~12s  %9.2f %9.3g) 
 label("Habitation fixed effects" "R-squared" "Observations"))
 starlevels(`"\sym{*}"' 0.10 `"\sym{**}"' 0.05 `"\sym{***}"' 0.01, label(" \(p<@\)"))
 varwidth(20)
 modelwidth(12)
 delimiter(&)
 end(\\)
 prehead(`"{"' `"\def\sym#1{\ifmmode^{#1}\else\(^{#1}\)\fi}"' `"\begin{tabular}{l*{@E}{c}}"' `"\toprule"')
 posthead("\midrule")
 prefoot("\midrule")
 postfoot(`"\bottomrule"' `"\multicolumn{@span}{l}{\footnotesize Standard errors clustered at the habitation level are in parentheses.}\\"' 
 `"\multicolumn{@span}{l}{\footnotesize @starlegend}\\"' `"\end{tabular}"' `"}"')
 label
 varlabels(_cons Constant, end("" \addlinespace) nolast)
 mlabels("WTP" "WTP" "Male head only", titles span prefix(\multicolumn{@span}{c}{) suffix(}))
 numbers(\multicolumn{@span}{c}{( )})
 collabels(none)
 eqlabels(none)
 substitute(_ \_ "\_cons " \_cons)
 interaction(" $\times$ ")
 notype
 level(95)
 style(esttab)
 replace ;
#delimit cr
























