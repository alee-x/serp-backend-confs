
* Stata version 15
* 18 January 2021

clear all
use ./sample_data

******************************************************************************************
* Baseline summary statistics and test of balance for covariates
* Table 2 in the paper is a combination of the following 
// ssc install balancetable
gen treatment_A = (treatment==1)
gen treatment_B = (treatment==2)
gen age = 2015 - q201_birthyear
label variable age "Age"

balancetable (mean if treatment == 1) (mean if treatment == 2) (mean if treatment == 3) ///
(diff treatment_A if treatment != 3) (diff treatment_A if treatment != 2) (diff treatment_B if treatment != 1) ///
q111_gender age q210_educationlevel q217_expensesmonth q219_ifyes_howmuchinr q220_currentlyindebtedinr ///
 q202_householdmember  q212_readhindi    ///
q405_owntotallandacre   q411_mobilephone q503_numberofkerosenelamps q508_hours_uselighting spending_month_perlamp ///
using "./Tables/balancetable_short.tex", ///
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



******************************************************************************************
* Baseline summary statistics and test of balance for variables across treatments and seed gender

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
q111_gender age q210_educationlevel q217_expensesmonth q219_ifyes_howmuchinr q220_currentlyindebtedinr ///
 q202_householdmember  q212_readhindi    ///
q405_owntotallandacre   q411_mobilephone q503_numberofkerosenelamps q508_hours_uselighting spending_month_perlamp ///
using "./Tables/balancetable_treatandgenderseed_short.tex", ///
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


******************************************************************************************
******************************************************************************************
* MAIN REGRESSIONS 
* The impact of unincentivized and incentivized communications on WTP
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



***************************************************************************
xtset serialnumber 
eststo clear

mean q302_WTPsol if control == 1
matrix tablecoef = e(b)
local meanControl `=string(tablecoef[1,1], "%9.2f")'
display "`meanControl'"

***************************************************************************
IncenVsUninc "q302_WTPsol control communication"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
local coef2p `r(coef2p)'
eststo: quietly xtreg q302_WTPsol network communication, fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local Compp "`coef2p'" , replace
estadd local Compcoeff_int "", replace
estadd local Compse_int "" , replace
estadd local Compp_int "" , replace
estadd local Compcoeff_int2 "", replace
estadd local Compse_int2 "" , replace
estadd local Compp_int2 "" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local meanofcontrol "`meanControl'", replace


***************************************************************************
IncenVsUninc "q302_WTPsol control communication control_gender comm_gender"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
local coef2p `r(coef2p)'
local coef3 `r(coef3)'
local coef4 `r(coef4)'
local coef4p `r(coef4p)'
eststo: quietly xtreg q302_WTPsol network communication network_gender comm_gender,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local Compp "`coef2p'" , replace
estadd local Compcoeff_int "`coef3'", replace
estadd local Compse_int "`coef4'" , replace
estadd local Compp_int "`coef4p'" , replace
estadd local Compcoeff_int2 "", replace
estadd local Compse_int2 "" , replace
estadd local Compp_int2 "" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local meanofcontrol "`meanControl'", replace

***************************************************************************
IncenVsUninc "q302_WTPsol control communication control_gender comm_gender if q111_gender == 0"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
local coef2p `r(coef2p)'
local coef3 `r(coef3)'
local coef4 `r(coef4)'
local coef4p `r(coef4p)'
eststo: quietly xtreg q302_WTPsol network communication network_gender comm_gender ///
if q111_gender == 0, fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local Compp "`coef2p'" , replace
estadd local Compcoeff_int "`coef3'", replace
estadd local Compse_int "`coef4'" , replace
estadd local Compp_int "`coef4p'" , replace
estadd local Compcoeff_int2 "", replace
estadd local Compse_int2 "" , replace
estadd local Compp_int2 "" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local meanofcontrol "`meanControl'", replace


***************************************************************************
IncenVsUninc "q302_WTPsol control communication control_same_gender comm_same_gender same_gender"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
local coef2p `r(coef2p)'
local coef5 `r(coef5)'
local coef6 `r(coef6)'
local coef6p `r(coef6p)'
eststo: quietly xtreg q302_WTPsol network communication  ///
network_same_gender  comm_same_gender same_gender ,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local Compp "`coef2p'" , replace
estadd local Compcoeff_int "", replace
estadd local Compse_int "" , replace
estadd local Compp_int "" , replace
estadd local Compcoeff_int2 "`coef5'", replace
estadd local Compse_int2 "`coef6'" , replace
estadd local Compp_int2 "`coef6p'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local meanofcontrol "`meanControl'", replace


// esttab using "./Tables/main_results.tex", ///
// replace b(%9.2f) stats(Compcoeff Compse fixedeffects secluster r2 N, fmt(%9.2f %9.2f %~12s %~12s %9.2f %9.2g) ///
// label("Incentivized vs. Unincentivized" " "   "Habitation fixed effects" "Clustered SE (habitation)" "R-squared" "Observations")) booktabs eqlabels(none) ///
// constant se label star(* 0.10 ** 0.05 *** 0.01) ///
// mtitles("WTP" "WTP" "Male Head Only" "WTP") noisily notype

#delimit ;
estout using "./Tables/main_results.tex" , 
 cells(b(fmt(%9.2f) star) se(fmt(%9.2f) par) p(fmt(%9.2f) par([ ])))
 stats(Compcoeff Compse Compp    Compcoeff_int Compse_int Compp_int   Compcoeff_int2 Compse_int2 Compp_int2   fixedeffects r2 N meanofcontrol, fmt(%9.2f %9.2f %9.2f %9.2f %9.2f %9.2f %9.2f %9.2f %9.2f %~12s  %9.2f %9.2g %9.2f) 
 label("Incentivized vs. Unincentivized" " " " "   "Incentivized vs. Unincentivized x Female Seed" " " " "   "Incentivized vs. Unincentivized x Same Gender" " "  " " "Habitation fixed effects"   "R-squared" "Observations" "Mean in Control"))
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
 mlabels("WTP" "WTP" "Male Head Only" "WTP", titles span prefix(\multicolumn{@span}{c}{) suffix(}))
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



******************************************************************************************
******************************************************************************************
* MAIN REGRESSION WITH SAVINGS
* The impact of unincentivized and incentivized communications on WTP controlling for savings
gen network_x_savings = network * q219_ifyes_howmuchinr
gen communication_x_savings = communication * q219_ifyes_howmuchinr
label variable network_x_savings "Unincentivized x Savings"
label variable communication_x_savings "Incentivized x Savings"
gen log_savings = log(q219_ifyes_howmuchinr+1)
gen network_x_log_savings = network * log_savings
gen communication_x_log_savings = communication * log_savings
label variable network_x_log_savings "Unincentivized x log Savings"
label variable communication_x_log_savings "Incentivized x log Savings"
label variable log_savings "Savings (log)"

mean q302_WTPsol if control == 1
matrix tablecoef = e(b)
local meanControl `=string(tablecoef[1,1], "%9.2f")'
display "`meanControl'"


xtset serialnumber 
eststo clear

IncenVsUninc "q302_WTPsol control communication q219_ifyes_howmuchinr"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q302_WTPsol network communication q219_ifyes_howmuchinr,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local meanofcontrol "`meanControl'", replace

IncenVsUninc "q302_WTPsol control communication q219_ifyes_howmuchinr network_x_savings communication_x_savings"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q302_WTPsol network communication q219_ifyes_howmuchinr  ///
network_x_savings communication_x_savings,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local meanofcontrol "`meanControl'", replace

IncenVsUninc "q302_WTPsol control communication log_savings"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q302_WTPsol network communication log_savings,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local meanofcontrol "`meanControl'", replace

IncenVsUninc "q302_WTPsol control communication log_savings network_x_log_savings communication_x_log_savings"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q302_WTPsol network communication log_savings  ///
network_x_log_savings communication_x_log_savings,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local meanofcontrol "`meanControl'", replace

IncenVsUninc "q302_WTPsol control communication if q219_ifyes_howmuchinr == 0"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q302_WTPsol network communication if q219_ifyes_howmuchinr == 0,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local meanofcontrol "`meanControl'", replace

// esttab using "./Tables/robustness_controls_savings.tex", ///
//  replace b(%9.2f) stats(Compcoeff Compse fixedeffects secluster r2 N, fmt(%9.2f %9.2f %~12s %~12s %9.2f %9.3g) ///
// label("Incentivized vs. Unincentivized" " "  "Habitation fixed effects" "Clustered SE (habitation)" "R-squared" "Observations")) booktabs eqlabels(none) ///
// constant se label star(* 0.10 ** 0.05 *** 0.01) nomtitles ///
// addnote ("Model 5 is for the sub-sample of respondents that declare zero savings.")  noisily notype


#delimit ;
estout using "./Tables/robustness_controls_savings.tex",
 cells(b(fmt(%9.2f) star) se(fmt(%9.2f) par))
 stats(Compcoeff Compse  fixedeffects   r2 N meanofcontrol, fmt(%9.2f %9.2f %~12s  %9.2f %9.3g  %9.2f) 
 label("Incentivized vs. Unincentivized" " " "Habitation fixed effects"  "R-squared" "Observations"  "Mean in Control"))
 starlevels(`"\sym{*}"' 0.10 `"\sym{**}"' 0.05 `"\sym{***}"' 0.01, label(" \(p<@\)"))
 varwidth(20)
 modelwidth(12)
 delimiter(&)
 end(\\)
 prehead(`"{"' `"\def\sym#1{\ifmmode^{#1}\else\(^{#1}\)\fi}"' `"\begin{tabular}{l*{@E}{c}}"' `"\toprule"')
 posthead("\midrule")
 prefoot("\midrule")
 postfoot(`"\bottomrule"' `"\multicolumn{@span}{l}{\footnotesize Standard errors clustered at the habitation level are in parentheses.}\\"' 
 `"\multicolumn{@span}{l}{\footnotesize Model 5 is for the sub-sample of respondents that declare zero savings.}\\"' 
 `"\multicolumn{@span}{l}{\footnotesize @starlegend}\\"' `"\end{tabular}"' `"}"')
 label
 varlabels(_cons Constant, end("" \addlinespace) nolast)
 mlabels(none)
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




******************************************************************************************
******************************************************************************************
* MAIN REGRESSIONS WITH CONTROLS
* The impact of unincentivized and incentivized communications on WTP controlling for more covariates
gen network_x_gender = network * q111_gender
gen communication_x_gender = communication * q111_gender
label variable spending_month "Monthly spending on lighting"
label variable network_x_gender "Unincentivized x Female Head"
label variable communication_x_gender "Incnetivized x Female Head"
label variable q111_gender "Female Head"


mean q302_WTPsol if control == 1
matrix tablecoef = e(b)
local meanControl `=string(tablecoef[1,1], "%9.2f")'
display "`meanControl'"


xtset serialnumber 
eststo clear

IncenVsUninc "q302_WTPsol control communication"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
quietly xtreg q302_WTPsol network communication q103_dateofinterview q111_gender q219_ifyes_howmuchinr q210_educationlevel q217_expensesmonth q220_currentlyindebtedinr ///
q202_householdmember q206_yourchildrengotoschool q503_numberofkerosenelamps q508_hours_uselighting  spending_month, fe vce(cluster serialnumber)
eststo: quietly xtreg q302_WTPsol network communication  if e(sample),  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local meanofcontrol "`meanControl'", replace


IncenVsUninc "q302_WTPsol control communication q111_gender"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
quietly xtreg q302_WTPsol network communication q103_dateofinterview q111_gender q219_ifyes_howmuchinr q210_educationlevel q217_expensesmonth q220_currentlyindebtedinr ///
q202_householdmember q206_yourchildrengotoschool q503_numberofkerosenelamps q508_hours_uselighting  spending_month, fe vce(cluster serialnumber)
eststo: quietly xtreg q302_WTPsol network communication q103_dateofinterview if e(sample),  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local meanofcontrol "`meanControl'", replace


IncenVsUninc "q302_WTPsol control communication q111_gender"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
quietly xtreg q302_WTPsol network communication q103_dateofinterview q111_gender q219_ifyes_howmuchinr q210_educationlevel q217_expensesmonth q220_currentlyindebtedinr ///
q202_householdmember q206_yourchildrengotoschool q503_numberofkerosenelamps q508_hours_uselighting  spending_month, fe vce(cluster serialnumber)
eststo: quietly xtreg q302_WTPsol network communication q111_gender  if e(sample),  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local meanofcontrol "`meanControl'", replace


IncenVsUninc "q302_WTPsol control communication q111_gender q219_ifyes_howmuchinr"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
quietly xtreg q302_WTPsol network communication q103_dateofinterview q111_gender q219_ifyes_howmuchinr q210_educationlevel q217_expensesmonth q220_currentlyindebtedinr ///
q202_householdmember q206_yourchildrengotoschool q503_numberofkerosenelamps q508_hours_uselighting  spending_month, fe vce(cluster serialnumber)
eststo: quietly xtreg q302_WTPsol network communication q111_gender q219_ifyes_howmuchinr  if e(sample),  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local meanofcontrol "`meanControl'", replace


IncenVsUninc "q302_WTPsol control communication q111_gender q219_ifyes_howmuchinr q210_educationlevel q217_expensesmonth q220_currentlyindebtedinr q202_householdmember q206_yourchildrengotoschool q503_numberofkerosenelamps q508_hours_uselighting  spending_month"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
quietly xtreg q302_WTPsol network communication q103_dateofinterview q111_gender q219_ifyes_howmuchinr q210_educationlevel q217_expensesmonth q220_currentlyindebtedinr ///
q202_householdmember q206_yourchildrengotoschool q503_numberofkerosenelamps q508_hours_uselighting  spending_month, fe vce(cluster serialnumber)
eststo: quietly xtreg q302_WTPsol network communication /// 
q111_gender q219_ifyes_howmuchinr q210_educationlevel q217_expensesmonth q220_currentlyindebtedinr ///
q202_householdmember q206_yourchildrengotoschool q503_numberofkerosenelamps q508_hours_uselighting  spending_month  if e(sample),  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local meanofcontrol "`meanControl'", replace


IncenVsUninc "q302_WTPsol control communication q111_gender q219_ifyes_howmuchinr q210_educationlevel q217_expensesmonth q220_currentlyindebtedinr q202_householdmember q206_yourchildrengotoschool q503_numberofkerosenelamps q508_hours_uselighting  spending_month"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
quietly xtreg q302_WTPsol network communication q103_dateofinterview q111_gender q219_ifyes_howmuchinr q210_educationlevel q217_expensesmonth q220_currentlyindebtedinr ///
q202_householdmember q206_yourchildrengotoschool q503_numberofkerosenelamps q508_hours_uselighting  spending_month, fe vce(cluster serialnumber)
eststo: quietly xtreg q302_WTPsol network communication q103_dateofinterview /// 
q111_gender q219_ifyes_howmuchinr q210_educationlevel q217_expensesmonth q220_currentlyindebtedinr ///
q202_householdmember q206_yourchildrengotoschool q503_numberofkerosenelamps q508_hours_uselighting  spending_month  if e(sample),  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local meanofcontrol "`meanControl'", replace


// esttab using "./Tables/robustness_controls.tex", ///
// replace b(%9.2f) stats(Compcoeff Compse fixedeffects secluster r2 N, fmt(%9.2f %9.2f %~12s %~12s %9.2f %9.3g) ///
// label("Incentivized vs. Unincentivized" " " "Habitation fixed effects" "Clustered SE (habitation)" "R-squared" "Observations")) booktabs eqlabels(none) ///
// constant se label star(* 0.10 ** 0.05 *** 0.01) nomtitles noisily notype

#delimit ;
estout using `"./Tables/robustness_controls.tex"' ,
 cells(b(fmt(%9.2f) star) se(fmt(%9.2f) par))
 stats(Compcoeff Compse  fixedeffects   r2 N meanofcontrol, fmt(%9.2f %9.2f %~12s  %9.2f %9.3g  %9.2f) 
 label("Incentivized vs. Unincentivized" " " "Habitation fixed effects"  "R-squared" "Observations"  "Mean in Control"))
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
 mlabels(none)
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



******************************************************************************************
******************************************************************************************
* MECHANISM
* Effect of treatments on variables highlighting possible mechanisms
xtset serialnumber 
eststo clear

IncenVsUninc "q602_seenasolarlanternbefore control communication"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q602_seenasolarlanternbefore network communication,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace

IncenVsUninc "q603_knowsomeonewithasolarlanter control communication"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q603_knowsomeonewithasolarlanter network communication,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace

IncenVsUninc "q607_maintenance control communication"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q607_maintenance network communication,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace

IncenVsUninc "q608_solarlanterncostsinr control communication  "
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q608_solarlanterncostsinr network communication,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace

IncenVsUninc "q609_solarlanternisaninnovativep control communication  "
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q609_solarlanternisaninnovativep network communication,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace

IncenVsUninc "q610_solarlanternisasuperiorthan control communication  "
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q610_solarlanternisasuperiorthan network communication,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace

IncenVsUninc "q611_useasolarlanterninsteadofak control communication  "
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q611_useasolarlanterninsteadofak network communication,  ///
fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'", replace
estadd local Compse "`coef2'" , replace
estadd local secluster "Yes" , replace
estadd local fixedeffects "Yes" , replace

label variable q602_seenasolarlanternbefore "\rothead{If has seen a solar lantern before}"
label variable q603_knowsomeonewithasolarlanter "\rothead{Knows someone with a solar lantern}"
label variable q607_maintenance "\rothead{Believes maintenance is important}"
label variable q608_solarlanterncostsinr "\rothead{Estimated cost of the solar lantern}"
label variable q609_solarlanternisaninnovativep "\rothead{Believes it is innovative}"
label variable q610_solarlanternisasuperiorthan "\rothead{Believes it is superior to kerosene lamps}"
label variable q611_useasolarlanterninsteadofak "\rothead{Would recommend over ker. lamp}"

esttab using "./Tables/reg_mechanism.tex", ///
replace b(%9.2f) stats(Compcoeff Compse fixedeffects secluster r2 N, fmt(%9.2f %9.2f %~12s %~12s %9.2f %9.2g) ///
label("Incentivized vs. Unincentivized" " "  "Habitation fixed effects" "Clustered SE (habitation)" "R-squared" "Observations")) booktabs eqlabels(none) ///
constant se label star(* 0.10 ** 0.05 *** 0.01) nonumbers  noisily notype

#delimit ;
estout using `"./Tables/reg_mechanism.tex"' ,
 cells(b(fmt(%9.2f) star) se(fmt(%9.2f) par))
 stats(Compcoeff Compse  fixedeffects   r2 N, fmt(%9.2f %9.2f %~12s  %9.2f %9.3g) 
 label("Incentivized vs. Unincentivized" " " "Habitation fixed effects"  "R-squared" "Observations"))
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
 mlabels(, depvar span prefix(\multicolumn{@span}{c}{) suffix(}))
 nonumbers
 collabels(none)
 eqlabels(none)
 substitute(_ \_ "\_cons " \_cons)
 interaction(" $\times$ ")
 notypelasso
 level(95)
 style(esttab)
 replace ;
#delimit cr

label variable q602_seenasolarlanternbefore "If has seen a solar lantern before"
label variable q603_knowsomeonewithasolarlanter "Knows someone with a solar lantern"
label variable q607_maintenance "Believes maintenance is important"
label variable q608_solarlanterncostsinr "Estimated cost of the solar lantern"
label variable q609_solarlanternisaninnovativep "Believes it is innovative"
label variable q610_solarlanternisasuperiorthan "Believes it is superior to kerosene lamps"
label variable q611_useasolarlanterninsteadofak "Would recommend over ker. lamp"




******************************************************************************************
******************************************************************************************
* MEDIATION ANALYSIS
* Following Acharya et al 2016
* sequential g-estimate 
xtset serialnumber 
gen ytilde = .

* Possible mediators...
sum q602_seenasolarlanternbefore q603_knowsomeonewithasolarlanter q607_maintenance spending_month q508_hours_uselighting if control == 0
tab treatment q602_seenasolarlanternbefore if control == 0
tab treatment q603_knowsomeonewithasolarlanter if control == 0
tab treatment q607_maintenance if control == 0
* Problem: there is really littl evariation in most of these variables... 
* it is unlikely that the sequential g-estimate gives anything

* Main Regression
// xtreg q302_WTPsol network communication, fe vce(cluster serialnumber)
* Network: 119.88 [76.27 - 163.50]
* Communication: 195.08 [149.87 - 240.29]

global controlvar
global controlvar $controlvar q111_gender q219_ifyes_howmuchinr q210_educationlevel q217_expensesmonth q220_currentlyindebtedinr q202_householdmember q206_yourchildrengotoschool q503_numberofkerosenelamps q508_hours_uselighting  spending_month

* DEFINE PROGRAM
capture program drop deboot
program define deboot, rclass
xtset serialnumber 
xtreg q302_WTPsol network communication `1' `2', fe vce(cluster serialnumber)
replace ytilde = q302_WTPsol - _b[`1'] * `1'
xtreg ytilde network communication, fe vce(cluster serialnumber)
return scalar deffect1 = _b[network]
return scalar deffect2 = _b[communication]
end


capture program drop deboot2
program define deboot2, rclass
xtset serialnumber 
xtreg q302_WTPsol control communication `1' `2', fe vce(cluster serialnumber)
replace ytilde = q302_WTPsol - _b[`1'] * `1'
xtreg ytilde control communication, fe vce(cluster serialnumber)
return scalar deffect1 = _b[control]
return scalar deffect2 = _b[communication]
end


capture program drop IncenVsUnincMediator
* this program outputs the coeff and se of Incentivized vs Unincentivized treatment
* this will be added to the main table as the last line
program define IncenVsUnincMediator, rclass
				bootstrap _b, reps(1000) seed(12345): deboot2 `1' `2'
				local coef1 `=string(_b[communication], "%9.2f")' 
				matrix tablecoef = r(table)
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
				return local coef1 `"`coef1'"'
				return local coef2 `"`coef2'"'
				
end

* All the sequential g-estimates for the dummy mediators ( q602, 603, 607 ) 

xtset serialnumber 
eststo clear

IncenVsUninc "q302_WTPsol control communication "
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: quietly xtreg q302_WTPsol network communication, fe vce(cluster serialnumber)
estadd local Compcoeff "`coef1'"
estadd local Compse "`coef2'"
estadd local bootstrap "No" , replace
estadd local fixedeffects "Yes" , replace
estadd local mediator "None" , replace


IncenVsUnincMediator "q602_seenasolarlanternbefore" "$controlvar"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: bootstrap _b, reps(1000) seed(12345): deboot "q602_seenasolarlanternbefore" "$controlvar"
estadd local Compcoeff `coef1'
estadd local Compse `coef2'
estadd local bootstrap "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local mediator "Seen One" , replace


IncenVsUnincMediator "q603_knowsomeonewithasolarlanter" "$controlvar"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: bootstrap _b, reps(1000) seed(12345): deboot "q603_knowsomeonewithasolarlanter" "$controlvar"
estadd local Compcoeff "`coef1'"
estadd local Compse "`coef2'"
estadd local bootstrap "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local mediator "Know Owner" , replace


IncenVsUnincMediator "q607_maintenance" "$controlvar"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: bootstrap _b, reps(1000) seed(12345): deboot "q607_maintenance" "$controlvar"
estadd local Compcoeff "`coef1'"
estadd local Compse "`coef2'"
estadd local bootstrap "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local mediator "Maintenance" , replace


IncenVsUnincMediator "spending_month" "$controlvar"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: bootstrap _b, reps(1000) seed(12345): deboot "spending_month" "$controlvar"
estadd local Compcoeff "`coef1'"
estadd local Compse "`coef2'"
estadd local bootstrap "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local mediator "Kerosene" , replace


IncenVsUnincMediator "q508_hours_uselighting" "$controlvar"
local coef1 `r(coef1)'
local coef2 `r(coef2)'
eststo: bootstrap _b, reps(1000) seed(12345): deboot "q508_hours_uselighting" "$controlvar"
estadd local Compcoeff "`coef1'"
estadd local Compse "`coef2'"
estadd local bootstrap "Yes" , replace
estadd local fixedeffects "Yes" , replace
estadd local mediator "Lighting" , replace

esttab using "./Tables/mediation.tex", ///
 replace b(%9.2f) stats(Compcoeff Compse mediator fixedeffects  bootstrap  r2 N, fmt(%9.2f %9.2f %~12s %~12s %~12s %9.2f %9.3g) ///
label("Incentivized vs. Unincentivized" " " "Mediator" "Habitation fixed effects" "SE Bootstrapped" "R-squared" "Observations")) booktabs eqlabels(none) ///
constant se label star(* 0.10 ** 0.05 *** 0.01) mtitles("ATE" "ACDE" "ACDE" "ACDE" "ACDE" "ACDE") noisily notype

#delimit ;
estout using `"./Tables/mediation.tex"' ,
 cells(b(fmt(%9.2f) star) se(fmt(%9.2f) par))
 stats(Compcoeff Compse mediator fixedeffects   bootstrap  r2 N, fmt( %9.2f   %9.2f  %~12s   %~12s %~12s %9.2f %9.3g) 
 label("Incentivized vs. Unincentivized" " "  "Mediator" "Habitation fixed effects"  "SE Bootstrapped" "R-squared" "Observations"))
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
 mlabels("ATE" "ACDE" "ACDE" "ACDE" "ACDE" "ACDE", titles span prefix(\multicolumn{@span}{c}{) suffix(}))
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



******************************************************************************************
******************************************************************************************
* Summary statistics on the status of women
balancetable ///
(mean if Seed_gender == 0) ///
(mean if Seed_gender == 1) /// 
(diff Seed_gender) ///
q700_a_thelocalhealthcenter q700_b_thehomeofrelativesorfrien q700_c_thelocalshopormarket q701_youandyourspousetalkaboutsp  ///
q702_womanshouldsayonhouseholdsp q703_importantthatgirlsgotoschoo q704_womenshouldworkoutside q705_a_ifshegoesoutwithouttellin ///
q705_b_ifsheargueswithherhusband q705_c_havingrelationswithotherm q803_arebetterabletousenewtechno ///
using "./Tables/balancetable_womenstatus.tex", ///
replace varlabels noobs booktabs  format(%9.2f)  nonumbers  ///
ctitles("Male Seed Friends" "Female Seed Friends" "Difference") 


