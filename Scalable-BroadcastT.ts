// Muaz Khan      - www.MuazKhan.com
// MIT License    - www.WebRTC-Experiment.com/licence
// Documentation  - github.com/muaz-khan/RTCMultiConnection

import { Socket } from 'net';

interface IUser {
    userid: string;
    broadcastId: string;
    typeOfStreams: {audio: boolean, video: boolean};
    receivingFrom: string;
    socket: Socket;
    canRelay: boolean;
    maxRelayLimitPerUser: number;

    relayReceivers?: any[];
    lastRelayuserid?: string;
    isBroadcastInitiator?: boolean;
}

const users: {[userid: string]: IUser} = {};

export const ScalableBroadcast = (socket, maxRelayLimitPerUser) => {
    try {
        maxRelayLimitPerUser = parseInt(maxRelayLimitPerUser) || 2;
    } catch (e) {
        maxRelayLimitPerUser = 2;
    }

    socket.on('join-broadcast', (user: IUser) => {
        try {
            if (!users[user.userid]) {
                socket.userid = user.userid;
                socket.isScalableBroadcastSocket = true;

                users[user.userid] = {
                    userid: user.userid,
                    broadcastId: user.broadcastId,
                    isBroadcastInitiator: false,
                    maxRelayLimitPerUser: maxRelayLimitPerUser,
                    relayReceivers: [],
                    receivingFrom: null,
                    canRelay: false,
                    typeOfStreams: user.typeOfStreams || { audio: true, video: true },
                    socket: socket
                };

                notifyBroadcasterAboutNumberOfViewers(user.broadcastId);
            }

            const relayUser = getFirstAvailableBraodcater(user.broadcastId, maxRelayLimitPerUser);

            if (relayUser === 'ask-him-rejoin' as any /* TODO: fix */) {
                socket.emit('rejoin-broadcast', user.broadcastId);
                return;
            }

            if (relayUser && user.userid !== user.broadcastId) {
                const hintsToJoinBroadcast = {
                    typeOfStreams: relayUser.typeOfStreams,
                    userid: relayUser.userid,
                    broadcastId: relayUser.broadcastId
                };

                users[user.userid].receivingFrom = relayUser.userid;
                users[relayUser.userid].relayReceivers.push(
                    users[user.userid]
                );
                users[user.broadcastId].lastRelayuserid = relayUser.userid;

                socket.emit('join-broadcaster', hintsToJoinBroadcast);

                // logs for current socket
                socket.emit('logs', 'You <' + user.userid + '> are getting data/stream from <' + relayUser.userid + '>');

                // logs for target relaying user
                relayUser.socket.emit('logs', 'You <' + relayUser.userid + '>' + ' are now relaying/forwarding data/stream to <' + user.userid + '>');
            } else {
                users[user.userid].isBroadcastInitiator = true;
                socket.emit('start-broadcasting', users[user.userid].typeOfStreams);

                // logs to tell he is now broadcast initiator
                socket.emit('logs', 'You <' + user.userid + '> are now serving the broadcast.');
            }
        } catch (e) {
            console.info(e);
        }
    });

    socket.on('scalable-broadcast-message', function(message) {
        socket.broadcast.emit('scalable-broadcast-message', message);
    });

    socket.on('can-relay-broadcast', function() {
        if (users[socket.userid]) {
            users[socket.userid].canRelay = true;
        }
    });

    socket.on('can-not-relay-broadcast', function() {
        if (users[socket.userid]) {
            users[socket.userid].canRelay = false;
        }
    });

    socket.on('check-broadcast-presence', function(userid, callback) {
        // we can pass number of viewers as well
        try {
            callback(!!users[userid] && users[userid].isBroadcastInitiator === true);
        }
        catch (e) {
            console.info(e);
        }
    });

    socket.on('get-number-of-users-in-specific-broadcast', function(broadcastId, callback) {
        try {
            if (!broadcastId || !callback) return;

            if (!users[broadcastId]) {
                callback(0);
                return;
            }

            callback(getNumberOfBroadcastViewers(broadcastId));
        }
        catch (e) {}
    });

    function getNumberOfBroadcastViewers(broadcastId): number {
        try {
            let numberOfUsers = 0;
            Object.keys(users).forEach(function(uid) {
                const user = users[uid];
                if (user.broadcastId === broadcastId) {
                    numberOfUsers++;
                }
            });
            return numberOfUsers - 1;
        }
        catch (e) {
            return 0;
        }
    }

    function notifyBroadcasterAboutNumberOfViewers(broadcastId, userLeft?: boolean) {
        try {
            if (!broadcastId || !users[broadcastId] || !users[broadcastId].socket) return;
            let numberOfBroadcastViewers = getNumberOfBroadcastViewers(broadcastId);

            if (userLeft === true) {
                numberOfBroadcastViewers--;
            }

            users[broadcastId].socket.emit('number-of-broadcast-viewers-updated', {
                numberOfBroadcastViewers: numberOfBroadcastViewers,
                broadcastId: broadcastId
            });
        }
        catch (e) {}
    }

    socket.on('disconnect', function() {
        try {
            if (!socket.isScalableBroadcastSocket) return;

            const user = users[socket.userid];

            if (!user) return;

            if (user.isBroadcastInitiator === false) {
                notifyBroadcasterAboutNumberOfViewers(user.broadcastId, true);
            }

            if (user.isBroadcastInitiator === true) {
                console.info({
                    'initiator-left': true,
                    'userid': user.userid,
                    'broadcastId': user.broadcastId,
                    'isBroadcastInitiator': user.isBroadcastInitiator,
                    'relayReceivers': Object.keys(user.relayReceivers)
                });

                // need to stop entire broadcast?
                for (const n in users) {
                    const _user = users[n];

                    if (_user.broadcastId === user.broadcastId) {
                        _user.socket.emit('broadcast-stopped', user.broadcastId);
                    }
                }

                delete users[socket.userid];
                return;
            }

            if (user.receivingFrom || user.isBroadcastInitiator) {
                const parentUser = users[user.receivingFrom];

                if (parentUser) {
                    const newArray = [];
                    parentUser.relayReceivers.forEach(function(n) {
                        if (n.userid !== user.userid) {
                            newArray.push(n);
                        }
                    });
                    users[user.receivingFrom].relayReceivers = newArray;
                }
            }

            if (user.relayReceivers.length && user.isBroadcastInitiator === false) {
                askNestedUsersToRejoin(user.relayReceivers);
            }

            delete users[socket.userid];
        } catch (e) {
            console.info(e);
        }
    });
};

function askNestedUsersToRejoin(relayReceivers) {
    try {
        const usersToAskRejoin = [];

        relayReceivers.forEach(function(receiver) {
            if (!!users[receiver.userid]) {
                users[receiver.userid].canRelay = false;
                users[receiver.userid].receivingFrom = null;
                receiver.socket.emit('rejoin-broadcast', receiver.broadcastId);
            }

        });
    }
    catch (e) {
        console.info(e);
    }
}

function getFirstAvailableBraodcater(broadcastId, maxRelayLimitPerUser) {
    try {
        const broadcastInitiator = users[broadcastId];

        // if initiator is capable to receive users
        if (broadcastInitiator.relayReceivers.length < maxRelayLimitPerUser) {
            return broadcastInitiator;
        }

        // otherwise if initiator knows who is current relaying user
        if (broadcastInitiator.lastRelayuserid) {
            const lastRelayUser = users[broadcastInitiator.lastRelayuserid];
            if (lastRelayUser && lastRelayUser.relayReceivers.length < maxRelayLimitPerUser) {
                return lastRelayUser;
            }
        }

        // otherwise, search for a user who not relayed anything yet
        let userFound: IUser;
        for (const n in users) {
            const user = users[n];

            if (userFound) {
                continue;
            }
            else if (user.broadcastId === broadcastId) {
                if (!user.relayReceivers.length && user.canRelay === true) {
                    userFound = user;
                }
            }
        }

        if (userFound) {
            return userFound;
        }

        // need to increase "maxRelayLimitPerUser" in this situation
        // so that each relaying user can distribute the bandwidth
        return broadcastInitiator;
    } catch (e) {
        console.info(e);
    }
}

function consoleInfo() {
    // return; // comment this line for development testings

    // console.info(arguments);
}
