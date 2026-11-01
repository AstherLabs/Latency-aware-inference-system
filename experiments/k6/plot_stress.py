#!/usr/bin/env python3
"""Plot sustained-overload evidence without equating rejection with success."""
import argparse
import csv
import json
from pathlib import Path
import re
import statistics

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np


def metric(text, name, status=None):
    total=0
    for line in text.splitlines():
        if not (line.startswith(name+' ') or line.startswith(name+'{')):continue
        if status is not None and f'status_code="{status}"' not in line:continue
        total+=float(line.rsplit(' ',1)[1])
    return total


def evaluate(report, folder):
    """Separate load validity, expected shedding/deadlines, and recovery checks."""
    stages=report['measurements']
    checked=report.get('acceptance',{}).copy()
    checked['zero_result_mismatches']=all(s['metrics'].get('result_mismatches',{}).get('values',{}).get('count',0)==0 for s in stages)
    soak=next(s for s in stages if s['phase']=='soak')
    rows=json.loads((folder/Path(soak['path']).name/'api-resources.json').read_text())
    start=report['soak_first_observed_arrival_epoch_s']
    stable=[r for r in rows if start+120<=r['epoch_s']<=start+report['soak_duration_s']]
    early=[r['rss_mib'] for r in stable if r['epoch_s']<=start+240]
    late=[r['rss_mib'] for r in stable if r['epoch_s']>=start+report['soak_duration_s']-120]
    slope=float(np.polyfit([(r['epoch_s']-start)/60 for r in stable],[r['rss_mib'] for r in stable],1)[0]) if len(stable)>2 else None
    resources={'sampled_peak_rss_mib':max(r['rss_mib'] for r in rows),
        'steady_rss_slope_mib_per_minute':slope,
        'early_steady_median_rss_mib':statistics.median(early) if early else None,
        'late_steady_median_rss_mib':statistics.median(late) if late else None,
        'minimum_threads':min(r['threads'] for r in rows),'maximum_threads':max(r['threads'] for r in rows),
        'maximum_fds':max(r['fds'] for r in rows),
        'sampled_peak_generator_rss_mib':max((r.get('generator_rss_mib',0) for r in rows),default=0)}
    return {'checks':checked,'functional_acceptance_passed':all(checked.values()),'resources':resources}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report',type=Path)
    args=parser.parse_args()
    path=args.report.resolve();folder=path.parent;report=json.loads(path.read_text())
    if not report.get('experiment_completed'):raise ValueError('Cannot publish incomplete stress run as final')
    assessment=evaluate(report,folder)
    (folder/'assessment.json').write_text(json.dumps(assessment,indent=2))
    records=[];telemetry=[];resources=[]
    for stage in report['measurements']:
        location=folder/Path(stage['path']).name
        records.append({key:stage.get(key) for key in ['phase','target_rps','sent','successes','rejected_503','deadline_504',
            'transport_failures','other_service_failures','generator_dropped','valid_load','success_p95_ms','completion_rps_over_execution']})
        for line in (location/'telemetry.jsonl').read_text().splitlines():
            row=json.loads(line)
            if 'metrics' in row:telemetry.append(row)
        resources.extend(json.loads((location/'api-resources.json').read_text()))
    with (folder/'stress-summary.csv').open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(records[0]));writer.writeheader();writer.writerows(records)
    telemetry.sort(key=lambda r:r['epoch_s']);resources.sort(key=lambda r:r['epoch_s'])
    beginning=resources[0]['epoch_s']
    plt.rcParams.update({'font.size':10,'axes.spines.top':False,'figure.facecolor':'white'})
    fig,axes=plt.subplots(4,1,figsize=(13,12),sharex=True)
    x=np.array([(r['epoch_s']-beginning)/60 for r in telemetry])
    times=np.array([r['epoch_s'] for r in telemetry])
    for status,color in [('200','#247c94'),('503','#c56b38'),('504','#b13939')]:
        counts=np.array([metric(r['metrics'],'predict_responses_total',status) for r in telemetry])
        axes[0].plot(x[1:],np.maximum(0,np.diff(counts))/np.maximum(.001,np.diff(times)),label='HTTP '+status,color=color,alpha=.9)
    axes[0].set_ylabel('Completions / second');axes[0].legend(frameon=False,ncol=3)
    rx=[(r['epoch_s']-beginning)/60 for r in resources]
    axes[1].plot(rx,[r['rss_mib'] for r in resources],color='#247c94',label='API sampled RSS')
    axes[1].set_ylabel('API memory (MiB)');axes[1].legend(frameon=False)
    for name,label,color in [('inference_api_queue_depth','Central queue','#c56b38'),
                             ('inference_sync_requests_active','Synchronous handlers','#247c94')]:
        axes[2].plot(rx,[r[name] for r in resources],color=color,label=label,alpha=.8)
    axes[2].set_ylabel('Tasks / handlers');axes[2].legend(frameon=False,ncol=2)
    axes[3].plot(rx,[r['threads'] for r in resources],color='#247c94',label='API threads')
    axes[3].set_ylabel('Threads');axes[3].set_ylim(bottom=0)
    fd=axes[3].twinx();fd.plot(rx,[r['fds'] for r in resources],color='#c56b38',alpha=.7,label='API descriptors');fd.set_ylabel('File descriptors')
    handles,labels=axes[3].get_legend_handles_labels();h,l=fd.get_legend_handles_labels();axes[3].legend(handles+h,labels+l,frameon=False,ncol=2)
    for index,event in enumerate(report.get('fault_timeline',[])):
        t=(event['requested_epoch_s']-beginning)/60
        for ax in axes:ax.axvline(t,color='#555555',alpha=.35,linestyle='--')
        label=event['action']+' '+('all' if len(event['workers'])==4 else 'one')
        axes[0].text(t,.9 if index%2==0 else .7,label,transform=axes[0].get_xaxis_transform(),rotation=90,va='top',fontsize=8)
    start=(report['soak_first_observed_arrival_epoch_s']-beginning)/60
    end=start+report['soak_duration_s']/60
    for ax in axes:
        ax.axvspan(start,end,color='#247c94',alpha=.045)
        ax.grid(alpha=.15)
    axes[-1].set_xlabel('Elapsed minutes; shaded region = 400 RPS sustained overload')
    fig.suptitle('Current platform: sustained overload, worker outages and recovery',fontweight='bold',fontsize=15)
    fig.text(.5,.012,'4 simulated workers; 1024 synchronous handlers; queue=1000; batch=1. HTTP 503 is controlled refusal, 504 is deadline expiry.\n'
             'Generator delivery and successful-request latency are reported separately. Local shared-host evidence, not real-model capacity.',ha='center',fontsize=9)
    fig.tight_layout(rect=[0,.05,1,.95]);fig.savefig(folder/'stress-timeseries.png',dpi=160);fig.savefig(folder/'stress-timeseries.pdf');plt.close(fig)
    print(json.dumps(assessment,indent=2));print('FIGURE_PATH='+str(folder/'stress-timeseries.png'))


if __name__=='__main__':main()
