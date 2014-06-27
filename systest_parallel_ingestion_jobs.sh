let iscene=0
while [ $iscene -ne $Nscenes ]; do
    export THIS_SCENE=SCENE_DIR$iscene
    export THIS_SCENE=${!THIS_SCENE}
    echo THIS_SCENE=XXX${THIS_SCENE}YYY
    if [ -z $THIS_SCENE ]; then
        break
    fi
    qsub -lncpus=1 -v DATACUBE_ROOT=$DATACUBE_ROOT,SYSTEST_DIR=$SYSTEST_DIR,SCENE_DIR=$THIS_SCENE $DATACUBE_ROOT/ingestion_job.sh
    let iscene=iscene+1
done
