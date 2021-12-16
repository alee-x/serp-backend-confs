*use Thomson_Ch6_Data.dta, clear


* Models of MP Occupation by District Characteristics *	
logit landowner landgini1882 urbanization1882 catholic1880 ineq1882, r
margins, at(landgini1882=(0.45(0.05)0.9)) atmeans
marginsplot, recast(line) recastci(rline) plot1opts(lwidth(medium)) ciopts(lwidth(thin) lpattern(longdash)) ///
graphregion(color(white)) bgcolor(white) xsize(6) ysize(6) scheme(s2mono)


mlogit occ_79 landgini1882 urbanization1882 catholic1880 ineq1882, r b(1)
margins, at(landgini1882=(0.45(0.05)0.9)) atmeans
marginsplot, recastci(rline) ///
legend(pos(6) rows(2) size(small)) xsize(5) ysize(5) ///
plot1opts(lwidth(med) lpattern(solid) lcolor(black) msymbol(D) mcolor(black)) ///
ci1opts(lwidth(thin) lpattern(longdash) lcolor(black)) ///
plot2opts(lwidth(thin) lpattern(solid) lcolor(gs12) msymbol(Oh) mcolor(gs12)) ///
ci2opts(lwidth(tvhin) lpattern(longdash) lcolor(gs12)) ///
plot3opts(lwidth(thin) lpattern(solid) lcolor(gs8) msymbol(Sh) mcolor(gs8)) ///
ci3opts(lwidth(vthin) lpattern(longdash)  lcolor(gs8)) ///
plot4opts(lwidth(thin) lpattern(solid) lcolor(gs10) msymbol(Th) mcolor(gs10)) ///
ci4opts(lwidth(vthin) lpattern(longdash)  lcolor(gs10)) ///
plot5opts(lwidth(thin) lpattern(solid) lcolor(gs6) msymbol(dh) mcolor(gs6)) ///
ci5opts(lwidth(vthin) lpattern(longdash)  lcolor(gs6)) ///
graphregion(fcolor(white)) bgcolor(white) scheme(s2mono) ///
title("Land Inequality") xtitle("Land Gini") xlabel(0.45(0.15)0.9) ///
ytitle("Pr(Elected)") ylabel(0(0.25)1) 
*graph save occ_landgini, replace

margins, at(urbanization1882=(0.05(0.1)0.95)) atmeans
marginsplot, recastci(rline) ///
legend(pos(6) rows(2) size(small)) xsize(5) ysize(5) ///
plot1opts(lwidth(med) lpattern(solid) lcolor(black) msymbol(D) mcolor(black)) ///
ci1opts(lwidth(thin) lpattern(longdash) lcolor(black)) ///
plot2opts(lwidth(thin) lpattern(solid) lcolor(gs12) msymbol(Oh) mcolor(gs12)) ///
ci2opts(lwidth(tvhin) lpattern(longdash) lcolor(gs12)) ///
plot3opts(lwidth(thin) lpattern(solid) lcolor(gs8) msymbol(Sh) mcolor(gs8)) ///
ci3opts(lwidth(vthin) lpattern(longdash)  lcolor(gs8)) ///
plot4opts(lwidth(thin) lpattern(solid) lcolor(gs10) msymbol(Th) mcolor(gs10)) ///
ci4opts(lwidth(vthin) lpattern(longdash)  lcolor(gs10)) ///
plot5opts(lwidth(thin) lpattern(solid) lcolor(gs6) msymbol(dh) mcolor(gs6)) ///
ci5opts(lwidth(vthin) lpattern(longdash)  lcolor(gs6)) ///
graphregion(fcolor(white)) bgcolor(white) scheme(s2mono) ///
title("Urbanization") xtitle("Urban") xlabel(0.05(0.15)0.95) ///
ytitle("Pr(Elected)") ylabel(0(0.25)1) 
*graph save occ_urbanization, replace

grc1leg occ_landgini.gph occ_urbanization.gph, graphregion(fcolor(white) ycommon)
*graph save occ_combined.gph, replace
*graph export MLogit_Occ_Comb.pdf, replace


****************************************************
* Models of MPs' Party By District Characteristics *
****************************************************
mlogit party landgini1882 ineq1882 urbanization1882 catholic1880, r baseoutcome(1)
margins, at(landgini1882=(0.45(0.05)0.9)) atmeans
marginsplot

margins, at(urbanization1882=(0.05(0.1)0.95)) atmeans
marginsplot

* Other models for interest
logit pr_army_79 landgini1882 urbanization1882 catholic1880 ineq1882

logit pr_herrenhaus_79 landgini1882 urbanization1882 catholic1880 ineq1882


*************************************
* Modeling the votes - binary logit *
*************************************
logit binary_ztvote ineq1882 urbanization1882 landgini1882 rye cattle lightind heavyind trade

logit binary_ztvote ineq1882 urbanization1882 landgini1882 rye cattle lightind heavyind trade freetrade

logit binary_ztvote mp_occ1 mp_occ2 mp_occ3 mp_occ5 pr_army_79 pr_herrenhaus_79

logit binary_ztvote mp_occ1 mp_occ2 mp_occ3 mp_occ5 pr_army_79 pr_herrenhaus_79 ///
ineq1882 urbanization1882 landgini1882 rye cattle lightind heavyind trade freetrade


**************************************				
* Modeling the votes - Ordered logit *
**************************************
ologit ordvote ineq1882 urbanization1882 landgini1882 rye cattle lightind heavyind trade if ordvote<99
est store ec_factors

ologit ordvote ineq1882 urbanization1882 landgini1882 rye cattle lightind heavyind trade freetrade if ordvote<99
est store mass_factors

ologit ordvote mp_occ1 mp_occ2 mp_occ3 mp_occ5 pr_army_79 pr_herrenhaus_79 if ordvote<99
est store personal_factors

ologit ordvote prty1 prty3 if ordvote<99
est store personal_party_factors

ologit ordvote mp_occ1 mp_occ2 mp_occ3 mp_occ5 pr_army_79 pr_herrenhaus_79 ///
ineq1882 urbanization1882 landgini1882 rye cattle lightind heavyind trade freetrade if ordvote<99
est store personal_mass_factors

ologit ordvote prty1 prty3 ///
ineq1882 urbanization1882 landgini1882 rye cattle lightind heavyind trade freetrade if ordvote<99
est store party_mass_factors

esttab ec_factors mass_factors personal_factors personal_party_factors personal_mass_factors party_mass_factors, type


esttab ec_factors mass_factors personal_factors personal_party_factors personal_mass_factors party_mass_factors ///
using Tariff_Regs.tex, replace ///
order(mp_occ1 mp_occ2 mp_occ3 mp_occ5 pr_army_79 pr_herrenhaus_79 prty1 prty3 ///
freetrade ineq1882 urbanization1882 landgini1882 rye cattle lightind heavyind) ///
l nogaps compress b(a2) se star(* 0.10 ** 0.05 *** 0.01) type ///
addnotes("0 is No; 1 is Absent; 2 is Abstain; 3 is Yes." ///
"Professionals are the base category for Models 3 \& 5.")




*********************************************************
* Creating index of how privileged these districts were *
*********************************************************
gen supp_78 = ((wheat_to_80*1.05)+(rye_to_80*1.07)+(barley_to_80*1.03)+(oats_to_80*1.08))/agpop1882

sort ver_bez
by ver_bez: egen prov_occ1 = mean(mp_occ1)
label var prov_occ1 "Reg. Landowners"

by ver_bez: egen prov_occ4 = mean(mp_occ4)
label var prov_occ4 "Reg. White Collar"

by ver_bez: egen prov_occ5 = mean(mp_occ5)
label var prov_occ5 "Reg. Businessman"

by ver_bez: egen prov_army = mean(pr_army_79)
label var prov_army "Reg. Pr Army"

by ver_bez: egen prov_herrenhaus = mean(pr_herrenhaus_79)
label var prov_herrenhaus "Reg. Pr UH"

by ver_bez: egen prov_sapd = mean(prty4)
label var prov_sapd "Reg. SAPD"

by ver_bez: egen prov_cons = mean(prty1)
label var prov_cons "Reg. Conserv"

by ver_bez: egen prov_libs = mean(prty2)
label var prov_libs "Reg. Liberal"

by ver_bez: egen prov_zen = mean(prty3)
label var prov_zen "Reg. Zentrum"

by ver_bez: egen prov_cons_90 = mean(cons_90)
label var prov_cons_90 "Cons. Vote 1890"

drop if provgini==.

graph twoway (scatter supp_78 provgini if prov_occ1>0.5, msymbol(Oh) mcolor(black)) ///
(scatter supp_78 provgini if prov_occ1<=0.5, msymbol(Dh) mcolor(gray)) ///
(lfit supp_78 provgini, lpattern(solid)), ///
xtitle("Landholding Inequality") ytitle("Weighted Ag Price Support") ///
graphregion(fcolor(white)) bgcolor(white) scheme(s2mono) xsize(5) ysize(5) ///
legend(size(small) pos(6) order(1 "Landowners > 50%" 2 "Landowners < 50%"))
graph export Landgini_W_Support.pdf, replace
