#include <iostream>
#include <stdio.h>
#include <queue>
#include <map>
#include <set>
#include <vector>
#include <string>

pthread_mutex_t fileLock;
pthread_barrier_t barrier;
//function to check if n is a perfect power
//for "powe
bool isPower(int n, int power) {
    long long num = n;
    int l = 1, r = n;
    int mid;
    long long exp;
    //binary search from 1 to n
    while (l <= r) {
        mid = l + (r - l) / 2;
        exp = 1;
        int i = 0;
        while (exp <= num && i < power) {
            exp = exp * mid;
            i++;
        }
        //if mid^power > n check the left side
        if (exp > num) {
            r = mid - 1;
        }
        //if mid^power > n check the right side
        else if (exp < num) {
            l = mid + 1;
        } else {
            return true;
        }
    }
    return false;
}
//argument for the mapper
typedef struct argMap {
    int maxPower;
    //queue with the name of the files
    std::queue<std::string> *files;
    //map with the power as key and a vector of
    //numbers that correspond to that power
    std::map<int, std::vector<int>> *powers;
} argMap;

void *mapFunc(void *arg) {
    argMap argument = *(argMap *) arg;
    for (int i = 2; i <= argument.maxPower; i++) {
        std::vector<int> vec;
        //declare a vector for each power
        argument.powers->insert(std::pair<int, std::vector<int>>(i, vec));
    }
    //while there are still files to be "mapped" in the queue
    while (argument.files->size() != 0) {
        //mutex to ensure that a single thread is extracting a file
        //at any given point
        pthread_mutex_lock(&fileLock);
        //in case 2 threads check the while condition at the same time
        //and the is only one file in the queue
        if (argument.files->size() == 0) {
            pthread_mutex_unlock(&fileLock);
            break;
        }
        std::string file = argument.files->front();
        argument.files->pop();
        pthread_mutex_unlock(&fileLock);
        int numbers, currNum;
        //open the file and read the number of numbers
        FILE *in = fopen(file.data(), "r");
        fscanf(in, "%d", &numbers);
        for (int i = 0; i < numbers; i++) {
            //read the number to be checked
            fscanf(in, "%d", &currNum);
            for (int j = 2; j <= argument.maxPower; j++) {
                if (isPower(currNum, j)) {
                    //put the perfect power in the map
                    argument.powers->at(j).push_back(currNum);
                }
            }
        }
        fclose(in);
    }
    pthread_barrier_wait(&barrier);
    return NULL;
}
//argument for reducer
typedef struct argRed {
    std::map<int, std::vector<int>> *powers;
    int exp, nMapThr;
} argRed;

void *reduceFunc(void *arg) {
    std::set<int> set;
    argRed argument = *(argRed *) arg;
    pthread_barrier_wait(&barrier);
    //take the numbers from the current power and put them
    //in a set
    for (int i = 0; i < argument.nMapThr; i++) {
        for (int j : argument.powers[i][argument.exp]) {
            set.insert(j);
        }
    }
    //write the size of the set in the file
    int toWrite = set.size();
    std::string file = "out";
    file += std::to_string(argument.exp) + ".txt";
    FILE *out = fopen(file.data(), "w");
    fprintf(out, "%d", toWrite);
    fclose(out);
    return NULL;
}

int main(int argc, char **argv) {
    if (argc != 4) {
        std::cout << "3 arguments required" << std::endl;
        return 1;
    }

    int nMapThr, nRedThr, noFiles;
    std::queue<std::string> files;
    FILE *in;
    //the number of Map threads
    nMapThr = atoi(argv[1]);
    //the number of Reducer threads
    nRedThr = atoi(argv[2]);
    std::map<int, std::vector<int>> powers[nMapThr];
    pthread_mutex_init(&fileLock, NULL);
    pthread_barrier_init(&barrier, NULL, nMapThr + nRedThr);
    in = fopen(argv[3], "r");
    fscanf(in, "%d", &noFiles);
    //read the name of the files
    for (int i = 0; i < noFiles; i++) {
        char currFile[50];
        fscanf(in, "%s", currFile);
        std::string currFileString(currFile);
        files.push(currFileString);
    }
    fclose(in);
    pthread_t threads[nMapThr + nRedThr];
    argMap mapArgs[nMapThr];
    for (int i = 0; i < nMapThr; i++) {
        mapArgs[i].files = &files;
        mapArgs[i].maxPower = nRedThr + 1;
        mapArgs[i].powers = &powers[i];
        
        int r = pthread_create(&threads[i], NULL, mapFunc, &mapArgs[i]);
        if (r) {
            std::cout << "Error creating thread " << i << std::endl;
            return 1;
        }
    }
    argRed redArgs[nRedThr];
    for (int i = 0; i < nRedThr; i++) {
        redArgs[i].powers = powers;
        redArgs[i].exp = i + 2;
        redArgs[i].nMapThr = nMapThr;

        int r = pthread_create(&threads[nMapThr + i], NULL, reduceFunc, &redArgs[i]);
        if (r) {
            std::cout << "Error creating thread " << i + nMapThr << std::endl;
            return 1;
        }
    }
    for (int i = 0; i < nMapThr + nRedThr; i++) {
        int r = pthread_join(threads[i], NULL);
        if (r) {
            std::cout << "Error joining thread " << i << std::endl;
            return 1;
        }
    }
    return 0;
}