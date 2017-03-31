# #!/bin/bash

# count=0
# hundred=31
# path="/home/liutao/backup_data/"
# for path1 in ${path}/*.8kb.hash.anon
# do
#     sudo ./destor "${path1}" 
#     sudo ./destor -g0
#     let count+=1
#     if [ $count -eq $hundred ]
#     then
#         break
#     fi
# done
# exit

# count=1
# hundred=32
# path="/home/liutao/backup_data/"
# for path1 in ${path}/*.8kb.hash.anon
# do
#     sudo ./destor "${path1}" 
#     sudo ./destor -g0
#     if [ $count -eq $hundred ]
#     then
#         break
#     fi
#     let count+=1
# done
# exit

# path="/home/liutao/backup_data/"
# for path1 in ${path}/*.8kb.hash.anon
# do
#     sudo ./destor "${path1}" 
#     sudo ./destor -g0
# done


#!/bin/bash

# path="/home/liutao/backup_data/"
# count=1
# hundred=3
# #jobid=0

# while [ ${count} -le ${hundred} ]
# do
#     #path1="${path}${count}"
#     sudo ./destor "${path}/*.hash.anon"
#     #sudo destor -r"${jobid}" "/home/wenjian/test"
#     count=`expr ${count} + 1`
#     #jobid=`expr ${jobid} + 1`
# done



# count=1
# hundred=4
#jobid=0


# while [ ${count} -le ${hundred} ]
# do
#     for path1 in ${path}/*
#     do
#         for path2 in ${path1}/*.8kb.hash.anon
#         do
#             sudo ./destor "${path2}"      
#         done
#     done
#     count=`expr ${count} + 1`
#     #jobid=`expr ${jobid} + 1`
# done

# for path1 in ${path}/*
# do
#     for path2 in ${path1}/*.8kb.hash.anon
#     do
#         sudo ./destor "${path2}"      
#     done
# done

# sudo ./destor /home/liutao/backup_data/macos-2011-06-23-001346.8kb.hash.anon
# sudo ./destor /home/liutao/backup_data/macos-2011-07-07-002651.8kb.hash.anon 
# sudo ./destor /home/liutao/backup_data/macos-2011-08-18-010012.8kb.hash.anon 



# count=1
# hundred=3
# #jobid=0

# while [ ${count} -le ${hundred} ]
# do
#     path1="${path}${count}"
#     sudo ./destor "${path1}/*/*.hash.anon"
#     #sudo destor -r"${jobid}" "/home/wenjian/test"
#     count=`expr ${count} + 1`
#     #jobid=`expr ${jobid} + 1`
# done


count=1
hundred=32
path="/home/liutao/backup_data/"
for path1 in ${path}/*.8kb.hash.anon
do
    sudo ./destor "${path1}" 
    # sudo ./destor -g0
    if [ $count -eq $hundred ]
    then
        break
    fi
    let count+=1
done
sudo ./destor -g0 
sudo ./destor -g1 
sudo ./destor -g2 
sudo ./destor -g3 
sudo ./destor -g4 
sudo ./destor -g5 
sudo ./destor -g6 
sudo ./destor -g7 
sudo ./destor -g8 
sudo ./destor -g9 
sudo ./destor -g10 
sudo ./destor -g11 
sudo ./destor -g12 
sudo ./destor -g13 
sudo ./destor -g14 
sudo ./destor -g15 
exit
