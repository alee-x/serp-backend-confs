import pandas as pd
import random
import datetime
import exrex
from collections import Counter
import numpy as np
from tqdm import tqdm
from random import randrange
from datetime import timedelta
from scipy.stats import truncnorm
import os
import sys

dob_range = [datetime.date(1920, 1, 1), datetime.date(2021,1,1)]
lsoa_rule = "W\d{8}"
# max, min, mean, median, std
epi_dur = [22000, 0, 4.1711898, 0, 56.963]
spell_dur = [22000, 1, 10.01916, 3, 95.9522363293]
epi_num = [99, 1, 1.206794, 1, 1.3431906]
dir = os.path.dirname(__file__)

def random_date(start = dob_range[0],end = dob_range[1]):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

def get_truncated_normal(mean=0, sd=1, low=0, upp=10):
    return truncnorm(
        (low - mean) / sd, (upp - mean) / sd, loc=mean, scale=sd)

# spell length, number of episodes
def get_episode_lengths(n, num_terms):
    num_terms = (num_terms or random.randint(2, n)) - 1
    a = random.sample(range(1, n), num_terms) + [0, n]
    list.sort(a)
    return [a[i+1] - a[i] for i in range(len(a) - 1)]


if __name__ == "__main__":

    part_name = sys.argv[1]

    datasets_to_generate = {
    "PEDW":["EPISODE", "SPELL", "SUPER_SPELL"],
    "WDSD":["MAIN"],
    "WLGP":["REG", "EVENT"]
    }
    generated_datasets = {
        "PEDW":[],
        "WDSD":[],
        "WLGP":[]
    }
    number_of_people = 10000
    while True:
        people_ids = {id_: random.randint(10_000_0000, 99_999_9999) for id_ in range(number_of_people)}
        if len(set(people_ids.values())) == number_of_people:
            # all the generated id's were unique
            break
        # otherwise this will repeat until they are
    pid_list = {x:[people_ids[x], random_date(), random.randint(0,1), exrex.getone(lsoa_rule)] for x in range(number_of_people)}
    wdsddf = pd.DataFrame([x for x in pid_list.values()], columns=['alf', 'wob', 'gndr_cd', 'lsoa2011_cd'])
    spsp_dist_list = Counter(list(people_ids.keys())[int(random.betavariate(2, 2)*len(list(people_ids.keys())))] for _ in range(number_of_people*10))
    wlgp_reg_dist = Counter(list(people_ids.keys())[int(random.betavariate(2, 2)*len(list(people_ids.keys())))] for _ in range(number_of_people*3))
    wlgp_event_dist = Counter(list(people_ids.keys())[int(random.betavariate(2, 2)*len(list(people_ids.keys())))] for _ in range(number_of_people*10))
    
    super_spell_store = []
    spell = []
    episode = []
    # Generate super spell first
    for i in range(number_of_people):
        # what is the normal distribution of their super spells?
        person_num_spsp = spsp_dist_list[i]
        pinfo = pid_list[i]
        for super_spell in range(person_num_spsp):
            # how many epi in spell
            num_epi = round(get_truncated_normal(mean=epi_num[2], sd=epi_num[4], low=epi_num[1], upp=epi_num[0]).rvs())
            person_spell_num = random.randint(10_000_000_00, 99_999_999_99)
            provider_spell_num = person_spell_num
            spell_num = random.randint(10_000_000_00, 99_999_999_99)
            for j in range(num_epi):
                super_spell_store.append([person_spell_num, provider_spell_num, spell_num, j+1])
            # figure out when they're admitted
            admis_dt = random_date(start=pinfo[1])
            fflag = True
            while fflag:
                spell_duration = round(get_truncated_normal(mean=spell_dur[2], sd=spell_dur[4], low=spell_dur[1], upp=spell_dur[0]).rvs())
                if num_epi <= spell_duration:
                    fflag = False
            disch_dt = admis_dt + timedelta(days=spell_duration)
            admis_yr = admis_dt.year
            disch_yr = disch_dt.year
            gndr_cd = pinfo[2]
            alf = pinfo[0]
            spell.append([spell_num, gndr_cd, admis_yr, admis_dt, disch_yr, disch_dt, spell_duration, alf])
            cumulative_spell_dur = 0
            if num_epi == 1:
                episode.append([spell_num, num_epi, admis_dt, disch_dt, spell_duration, 1])
            else:
                epi_lengths = get_episode_lengths(spell_duration, num_epi)
                for k in range(num_epi):
                    this_epi_len = epi_lengths[k]
                    if k == 0:
                        epi_end = admis_dt + timedelta(days=this_epi_len)
                        episode.append([spell_num, k+1, admis_dt, epi_end, this_epi_len, 1])
                        cumulative_spell_dur += this_epi_len
                    else:
                        epi_start = admis_dt + timedelta(days=cumulative_spell_dur)
                        epi_end = epi_start + timedelta(days=this_epi_len)
                        episode.append([spell_num, k+1, epi_start, epi_end, this_epi_len, 0])
    superspelldf = pd.DataFrame(super_spell_store, columns=['person_spell_num', 'provider_spell_num', 'spell_num', 'epi_num'])
    spelldf = pd.DataFrame(spell, columns=['spell_num', 'gndr_cd', 'admis_yr', 'admis_dt', 'disch_yr', 'disch_dt', 'spell_duration', 'alf'])
    episodedf = pd.DataFrame(episode, columns=['spell_num', 'epi_num', 'epi_start', 'epi_end', 'epi_duration', 'first_epi_in_spell'])
    
    wlgp_reg = []
    wlgp_event = []
    event_code_structure = "[a-zA-Z]\d[a-zA-Z]\d[a-zA-Z][.]{0,2}"
    ed = datetime.date(2021, 9, 28)
    for i in range(number_of_people):
        person_num_gp_reg = wlgp_reg_dist[i]
        pinfo = pid_list[i]
        alf = pinfo[0]
        dob = pinfo[1]
        gndr_cd = pinfo[2]
        if person_num_gp_reg > 0:
            gp_total_reg_lens = datetime.date(2021, 9, 28) - pinfo[1]
            regists = get_episode_lengths(gp_total_reg_lens.days, person_num_gp_reg)
            cumulative_regs = 0
            for reg in regists:
                start_dt = pinfo[1] + timedelta(days=cumulative_regs)
                end_dt = start_dt + timedelta(days=reg)
                wlgp_reg.append([alf, start_dt, end_dt])
                cumulative_regs += reg
        person_num_gp_event = wlgp_event_dist[i]
        # alf, gndr_cd, wob, event_dt, event_cd
        if person_num_gp_event > 0:
            for event in range(person_num_gp_event):
                event_dt = random_date(start=dob)
                event_cd = exrex.getone(event_code_structure)
                wlgp_event.append([alf, gndr_cd, dob, event_dt, event_cd])
    wlgpregdf = pd.DataFrame(wlgp_reg, columns=['alf', 'start_dt', 'end_dt'])
    wlgpeventdf = pd.DataFrame(wlgp_event, columns=['alf', 'gndr_cd', 'wob', 'event_dt', 'event_cd'])

    fpaths = ['minio']
    for path in fpaths:
        pri_string = 'db-startup-scripts/{0}/data/parts/'.format(path)
        path_str = os.path.join(dir, pri_string)
        pri_string = path_str
        wdsddf.to_csv(pri_string + 'wdsd_main_{0}.csv'.format(part_name), index=False)
        superspelldf.to_csv(pri_string + 'pedw_superspell_{0}.csv'.format(part_name), index=False)
        spelldf.to_csv(pri_string + 'pedw_spell_{0}.csv'.format(part_name), index=False)
        episodedf.to_csv(pri_string + 'pedw_episode_{0}.csv'.format(part_name), index=False)
        wlgpregdf.to_csv(pri_string + 'wlgp_reg_{0}.csv'.format(part_name), index=False)
        wlgpeventdf.to_csv(pri_string + 'wlgp_event_{0}.csv'.format(part_name), index=False)