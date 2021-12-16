clear all
set more off
cd "/home/andrei/Desktop/Dropbox/rolling elections/"
global controls partymark medianhha newstv newspaper illiterate graduate enrel enlang scres stres

**** combined
use lworking,clear
drop if thedate-ststart>10
quietly regress enlp later $controls lnmargin yr1 yr2 enlp_lag hlvote vs_*
keep if e(sample)
gen comp=-margin

eststo p_1: dirifit vs_abst vs_viab vs_nviab,muvar(later) robust
eststo p_2: dirifit vs_abst vs_viab vs_nviab,muvar(later partymark yr1 yr2) mu2(vs_viab_lag) mu3(vs_nviab_lag) robust
eststo p_3: dirifit vs_abst vs_viab vs_nviab,muvar(later $controls yr1 yr2) robust
eststo p_4: dirifit vs_abst vs_viab vs_nviab,muvar(later $controls yr1 yr2) mu2(vs_viab_lag) mu3(vs_nviab_lag)  robust
eststo p_5: dirifit vs_abst vs_viab vs_nviab,muvar(later $controls yr1 yr2 comp) mu2(vs_viab_lag) mu3(vs_nviab_lag) robust
eststo h_1: betafit hlvote,muvar(later)  robust
eststo h_2: betafit hlvote,muvar(later  partymark yr1 yr2 vs_viab_lag vs_nviab_lag) robust
eststo h_3: betafit hlvote,muvar(later $controls yr1 yr2) robust
eststo h_4: betafit hlvote,muvar(later $controls yr1 yr2 vs_viab_lag vs_nviab_lag) robust
eststo h_5: betafit hlvote,muvar(later $controls comp yr1 yr2 vs_viab_lag vs_nviab_lag) robust
eststo l_1: regress enlp later,robust
eststo l_2: regress enlp later partymark yr1 yr2 enlp_lag,robust
eststo l_3: regress enlp later $controls yr1 yr2,robust
eststo l_4: regress enlp later $controls yr1 yr2 enlp_lag,robust
eststo l_5: regress enlp later $controls comp yr1 yr2 enlp_lag,robust

esttab p_3 p_4 h_3 h_4 using "estimates.csv", ///
csv replace b(%10.3f) se stats(ll aic bic N, labels("Log lik." "AIC" "BIC" "Obs") fmt(%10.1f %10.1f %10.1f %10.0f)) starlevels(* 0.1 ** 0.05 *** 0.01) ///
varlabels(_cons Constant) alignment(l) unstack nogaps nodepvars label nonumbers compress nobase noomit obslast long ///
order(later partymark medianhha newstv newspaper illiterate graduate enrel enlang scres stres yr1 yr2 vs_viab_lag vs_nviab_lag _cons)

esttab l_3 l_4 using "estimatesA.csv", ///
csv replace b(%10.3f) se stats(r2 N, labels("R-squared" "Obs") fmt(%10.2f %10.0f)) starlevels(* 0.1 ** 0.05 *** 0.01) ///
varlabels(_cons Constant) alignment(l) unstack nogaps nodepvars label nonumbers compress nobase noomit obslast long  ///
order(later partymark medianhha newstv newspaper illiterate graduate enrel enlang scres stres yr1 yr2  enlp_lag _cons)

esttab p_1 p_2 p_3 p_4 p_5 using "estimates1.csv", ///
csv replace b(%10.3f) se stats(ll aic bic N, labels("Log lik." "AIC" "BIC" "Obs") fmt(%10.1f %10.1f %10.1f %10.0f)) starlevels(* 0.1 ** 0.05 *** 0.01) ///
varlabels(_cons Constant) alignment(l) unstack nogaps nodepvars label nonumbers compress nobase noomit obslast long ///
order(later partymark medianhha newstv newspaper illiterate graduate enrel enlang scres stres yr1 yr2 vs_viab_lag vs_nviab_lag _cons)

esttab h_1 h_2 h_3 h_4 h_5 using "estimates2.csv", ///
csv replace b(%10.3f) se stats(ll aic bic N, labels("Log lik." "AIC" "BIC" "Obs") fmt(%10.1f %10.1f %10.1f %10.0f)) starlevels(* 0.1 ** 0.05 *** 0.01) ///
varlabels(_cons Constant) alignment(l) nogaps nodepvars label nonumbers compress nobase noomit obslast long ///
order(later partymark medianhha newstv newspaper illiterate graduate enrel enlang scres stres yr1 yr2 vs_viab_lag vs_nviab_lag comp _cons)

esttab l_1 l_2 l_3 l_4 l_5 using "estimates3.csv", ///
csv replace b(%10.3f) se stats(r2 N, labels("R-squared" "Obs") fmt(%10.2f %10.0f)) starlevels(* 0.1 ** 0.05 *** 0.01) ///
varlabels(_cons Constant) alignment(l) nogaps nodepvars label nonumbers compress nobase noomit obslast long ///
order(later partymark medianhha newstv newspaper illiterate graduate enrel enlang scres stres yr1 yr2  enlp_lag comp _cons)

** out-of-sample predictions
use lworking,clear
drop if thedate-ststart>10
quietly regress enlp later $controls lnmargin yr1 yr2 enlp_lag hlvote vs_*
keep if e(sample)
gen comp=-margin
quietly dirifit vs_abst vs_viab vs_nviab,muvar(later $controls yr1 yr2) mu2(vs_viab_lag) mu3(vs_nviab_lag) robust
ddirifit,at(scres 0 stres 0 yr1 0 yr2 0)

quietly dirifit vs_abst vs_viab vs_nviab,muvar(later $controls yr1 yr2) mu2(vs_viab_lag) mu3(vs_nviab_lag) robust
matrix ests=e(b)
collapse (mean) $controls vs_*
replace stres=0
replace scres=0
gen yr1=0
gen yr2=0
expand 2
gen later=_n==1

matrix score xb2=ests,eq(mu2)
matrix score xb3=ests,eq(mu3)
matrix score lnphi=ests,eq(ln_phi)

gen prop2=exp(xb2)/(1+exp(xb2)+exp(xb3))
gen prop3=exp(xb3)/(1+exp(xb2)+exp(xb3))
gen prop1=1-prop2-prop3

** out-of-sample predictions
use lworking,clear
drop if thedate-ststart>10
quietly regress enlp later $controls lnmargin yr1 yr2 enlp_lag hlvote vs_*
keep if e(sample)
gen comp=-margin
quietly betafit hlvote,muvar(later $controls yr1 yr2 vs_viab_lag vs_nviab_lag) robust
dbetafit,at(scres 0 stres 0 yr1 0 yr2 0)

quietly betafit hlvote,muvar(later $controls yr1 yr2 vs_viab_lag vs_nviab_lag) robust
matrix ests=e(b)
collapse (mean) $controls vs_viab_lag vs_nviab_lag
replace stres=0
replace scres=0
gen yr1=0
gen yr2=0
expand 2
gen later=_n==1 
matrix score xb=ests,eq(mu)
gen prop=1/(1+exp(-xb))


** out-of-sample predictions
use lworking,clear
drop if thedate-ststart>10
quietly regress enlp later $controls lnmargin yr1 yr2 enlp_lag hlvote vs_*
keep if e(sample)
gen comp=-margin 

quietly regress enlp later $controls yr1 yr2 enlp_lag,robust
matrix ests=e(b)
collapse (mean) $controls enlp_lag
replace stres=0
replace scres=0
gen yr1=0
gen yr2=0
expand 2
gen later=_n==1 
matrix score xb=ests 

** summary statistics
use lworking,clear
drop if thedate-ststart>10
quietly regress enlp later $controls lnmargin yr1 yr2 enlp_lag hlvote vs_*
keep if e(sample)
gen comp=-margin

sutex vs_viab vs_nviab hlvote enlp later $controls yr1 yr2 vs_viab_lag vs_nviab_lag enlp_lag comp,digit(3) file(sumstat) replace minmax
