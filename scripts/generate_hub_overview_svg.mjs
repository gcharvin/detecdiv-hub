// Fixed 16:9 layout. SVG group IDs are animation hooks: keep them stable.
import fs from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.join(root,'docs/figures/detecdiv-hub-overview-animatable.svg');
const parts=[];
const C={ink:'#182437',muted:'#52637A',client:'#D97706',hub:'#2563EB',linux:'#7C3AED',windows:'#0891B2',storage:'#16A34A',archive:'#DB2777',external:'#B77909'};
const esc=s=>String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
function group(id,label,fn,{step,status,cls=''}={}){
  parts.push(`<g id="${id}" aria-label="${esc(label)}" class="${cls}"${step?` data-step="${step}"`:''}${status?` data-status="${status}"`:''}>`);
  fn();parts.push('</g>');
}
function box(x,y,w,h,fill,stroke,{dashed=false,r=12}={}){
  parts.push(`<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="${r}" fill="${fill}" stroke="${stroke}" stroke-width="2"${dashed?' stroke-dasharray="7 5"':''}/>`);
}
function text(x,y,t,size=22,bold=false,color=C.ink){
  parts.push(`<text fill="${color}" font-size="${size}" font-weight="${bold?'700':'400'}">${t.split('\n').map((s,i)=>`<tspan x="${x}" y="${y+size+i*size*1.25}">${esc(s)}</tspan>`).join('')}</text>`);
}
function flow(id,points,label,lx,ly,color,{dashed=false,both=false,step=1,size=18,cls=''}={}){
  group(id,label,()=>{
    parts.push(`<path id="${id}-path" class="flow-path" d="${points.map((p,i)=>`${i?'L':'M'} ${p[0]} ${p[1]}`).join(' ')}" fill="none" stroke="${color}" stroke-width="3"${dashed?' stroke-dasharray="7 5"':''}/>`);
    group(`${id}-heads`,'Arrowheads',()=>{
      function tip(p,q){const a=Math.atan2(p[1]-q[1],p[0]-q[0]),b=[p[0]-11*Math.cos(a),p[1]-11*Math.sin(a)];parts.push(`<polygon points="${p.join(',')} ${b[0]+5*Math.sin(a)},${b[1]-5*Math.cos(a)} ${b[0]-5*Math.sin(a)},${b[1]+5*Math.cos(a)}" fill="${color}"/>`);}
      tip(points.at(-1),points.at(-2));if(both)tip(points[0],points[1]);
    });
    group(`${id}-label`,label,()=>text(lx,ly,label,size,true,color));
  },{step,cls:`flow ${cls}`});
}

group('diagram-title','DetecDiv Hub',()=>{
  text(40,23,'DetecDiv Hub',44,true);
  text(955,37,'Acquire · catalogue · process · protect',25,false,C.muted);
});

group('clients','Microscope and user clients',()=>{
  box(40,115,350,265,'#FFF8EB',C.client);
  text(60,132,'Microscope & users',27,true,'#92400E');
  group('acquisition-client','pymmcore-gui microscope acquisition',()=>{
    box(60,177,310,77,'#FFFFFF','#EDD1A2');
    text(75,189,'pymmcore-gui',24,true);
    text(75,224,'Microscope acquisition',20);
  },{step:1});
  group('desktop-client','Browser and local DetecDiv',()=>{
    box(60,271,310,88,'#FFFFFF','#EDD1A2');
    text(75,280,'User workstation',23,true);
    text(75,313,'Browser + DetecDiv local\nOptional local MATLAB runs',19);
  },{step:2});
},{step:1});

group('hub-vm','Hub VM webserver-labo',()=>{
  box(470,115,655,265,'#EDF4FF',C.hub);
  text(490,132,'Hub VM · webserver-labo',28,true,'#1D4ED8');
  group('hub-api','Web UI and Hub API',()=>{
    box(490,177,260,180,'#FFFFFF','#BFD4F5');
    text(507,191,'Web UI + API',25,true);
    text(507,238,'User access\nDataset browser\nJob / assistant API',22);
  });
  group('hub-database','PostgreSQL catalogue and job queue',()=>{
    box(770,177,335,180,'#FFFFFF','#BFD4F5');
    text(790,191,'PostgreSQL',25,true);
    text(790,238,'Dataset / project catalogue\nJob queue & execution state\nLabguru links + code commits',21);
  });
},{step:3});

group('integrations','Connected systems',()=>{
  box(1205,115,355,265,'#FFFAE8',C.external);
  text(1225,132,'Connected systems',27,true,'#85590B');
  group('labguru','Labguru experiment and biological catalogue',()=>{
    box(1225,177,315,78,'#FFFFFF','#E4D39C');
    text(1240,187,'Labguru',24,true);
    text(1240,224,'Experiments · strains · stocks',20);
  });
  group('gitlab','GitLab versioned code and releases',()=>{
    box(1225,275,315,82,'#FFFFFF','#E4D39C');
    text(1240,285,'GitLab',24,true);
    text(1240,321,'Versioned code + releases',20);
  });
},{step:4});

group('storage','Shared storage on two NAS machines',()=>{
  box(40,455,570,340,'#F1FAF3',C.storage);
  text(60,474,'Shared storage · Synology NAS',28,true,'#126A32');
  group('primary-nas','Primary NAS data and homes',()=>{
    box(60,522,530,111,'#FFFFFF','#B9DCC3');
    text(78,534,'Primary NAS · 10.20.11.250',25,true);
    text(78,573,'/data — existing projects + raw data\n/homes — user homes + Hub folders',22);
  });
  group('secondary-nas','Secondary NAS homes2',()=>{
    box(60,652,530,121,'#FFFFFF','#B9DCC3');
    text(78,664,'Secondary NAS · 10.20.8.250',25,true);
    text(78,704,"/homes2 — Alexander's home\nRead-only · write setup pending",22);
  });
},{step:2});

group('execution-targets','Worker targets sharing the Hub job queue',()=>{
  box(690,455,435,340,'#F6F1FF',C.linux);
  text(710,472,'Execution targets',28,true,'#6D28D9');
  text(710,508,'Shared queue · separate machines',18,false,C.muted);
  group('linux-host','Primary Linux compute host',()=>{
    box(710,538,395,110,'#FFFFFF','#CBB6F1');
    group('linux-workers','Linux worker pool on detecdiv-server',()=>{
      text(727,546,'Linux · detecdiv-server',24,true,'#6D28D9');
      text(727,579,'MATLAB / Python · primary pool',19.5);
    });
    group('qwen-assistant','Planned local Qwen agent',()=>{
      box(726,609,363,29,'#FFF9F2','#D17B28',{dashed:true,r:6});
      text(735,612,'Qwen · projects + scripts (planned)',17.5,true,'#92561D');
    },{step:8,status:'planned'});
  },{step:5,cls:'worker-target linux-target'});
  group('windows-worker','Supplemental native Windows worker',()=>{
    box(710,660,395,74,'#EAFBFE',C.windows);
    text(727,667,'Windows · additional worker',22,true,'#087D99');
    text(727,699,'CG-PCDELL01-306 · one job at a time',18.5);
  },{step:6,cls:'worker-target windows-target',status:'additional'});
  group('future-workers','Future worker targets',()=>{
    box(710,746,395,28,'#FFFFFF','#94A3B8',{dashed:true,r:6});
    text(727,749,'+ Future targets · join the same queue',17.5,false,'#52637A');
  },{step:9,cls:'worker-target future-target',status:'planned'});
},{step:5});

group('archive-storage','Dedicated archive share',()=>{
  box(1205,455,355,340,'#FFF0F6',C.archive);
  text(1225,474,'Archive storage',28,true,'#B32169');
  text(1225,524,'10.20.11.251 · /archive',22,true);
  group('backup-task','Restic backup snapshots',()=>{
    text(1225,573,'BACKUP',20,true,'#B32169');
    text(1225,604,'Restic snapshots\nProjects + raw datasets',22);
  });
  group('archive-task','Raw dataset archiving',()=>{
    text(1225,680,'ARCHIVE',20,true,'#B32169');
    text(1225,711,'ZIP / tar.gz bundles\nOptional cleanup after success',19);
  });
},{step:7});

group('flows','Data and control flows',()=>{
  flow('flow-api',[[390,231],[470,231]],'API',406,195,C.hub,{dashed:true,both:true,step:3,cls:'control-flow'});
  flow('flow-labguru',[[1205,222],[1125,222]],'links',1142,184,C.external,{dashed:true,both:true,step:4,cls:'catalogue-flow'});
  flow('flow-client-files',[[230,380],[230,455]],'Images / Samba files',244,403,C.storage,{step:2,cls:'data-flow'});
  flow('flow-assistant',[[735,380],[735,455]],'assistant',748,403,C.hub,{dashed:true,both:true,step:8,cls:'assistant-flow'});
  flow('flow-jobs',[[950,380],[950,455]],'jobs',965,403,C.hub,{dashed:true,step:5,cls:'queue-flow'});
  flow('flow-code-release',[[1315,380],[1315,421],[1080,421],[1080,455]],'code release',1130,393,C.external,{dashed:true,step:5,cls:'code-flow'});
  flow('flow-worker-files',[[690,582],[610,582]],'files',628,544,C.storage,{both:true,step:5,cls:'data-flow'});
  flow('flow-protection',[[1125,623],[1205,623]],'backup /\narchive',1134,564,C.archive,{step:7,size:17,cls:'protection-flow'});
});

group('footer','Summary and planned feature note',()=>{
  text(40,823,'Images on NAS · shared Hub queue · Linux and Windows workers · code in GitLab',25,true);
  text(40,866,'Dashed boxes = planned. Windows claims eligible unassigned jobs. Scheduled Restic backups were disabled at the last documented check.',16.5,false,'#69778A');
});

const header=`<svg xmlns="http://www.w3.org/2000/svg" id="hub-diagram" width="100%" height="100%" viewBox="0 0 1600 900" preserveAspectRatio="xMidYMid meet" role="img" aria-labelledby="svg-title svg-description">
<title id="svg-title">DetecDiv Hub: clients, catalogue, worker targets and shared storage</title>
<desc id="svg-description">Landscape diagram with stable group IDs for animation. Linux workers run on detecdiv-server. The additional Windows worker runs on CG-PCDELL01-306, consumes eligible unassigned jobs from the same PostgreSQL queue, and processes one job at a time. Dashed future worker targets and Qwen project and scripting tools are planned. Hub filesystem indexing remains on Linux workers. Large image files move through shared storage. The archive share stores Restic backups and raw dataset bundles.</desc>
<metadata id="animation-map">{"format":"detecdiv-hub-animation.v1","canvas":[1600,900],"steps":{"1":"acquisition","2":"storage and desktop access","3":"Hub catalogue and API","4":"Labguru and GitLab","5":"Linux execution","6":"additional Windows worker","7":"backup and archive","8":"planned Qwen agent","9":"future worker targets"}}</metadata>
<style>text{font-family:Arial,Helvetica,sans-serif}.flow-path{stroke-linejoin:round;stroke-linecap:butt}#hub-diagram{shape-rendering:geometricPrecision}.worker-target{isolation:isolate}</style>
<rect id="slide-background" width="1600" height="900" fill="#FFFFFF"/>
`;
await fs.mkdir(path.dirname(output),{recursive:true});
await fs.writeFile(output,header+parts.join('\n')+'\n</svg>\n');
console.log(output);
