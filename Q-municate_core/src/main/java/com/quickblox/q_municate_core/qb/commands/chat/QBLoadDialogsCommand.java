package com.quickblox.q_municate_core.qb.commands.chat;

import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.util.Log;

import com.quickblox.chat.model.QBDialog;
import com.quickblox.chat.model.QBDialogType;
import com.quickblox.core.exception.QBResponseException;
import com.quickblox.core.request.QBRequestGetBuilder;
import com.quickblox.q_municate_core.core.command.ServiceCommand;
import com.quickblox.q_municate_core.models.ParcelableQBDialog;
import com.quickblox.q_municate_core.qb.helpers.QBGroupChatHelper;
import com.quickblox.q_municate_core.qb.helpers.QBPrivateChatHelper;
import com.quickblox.q_municate_core.service.QBService;
import com.quickblox.q_municate_core.service.QBServiceConsts;
import com.quickblox.q_municate_core.utils.ChatUtils;
import com.quickblox.q_municate_core.utils.ConstsCore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class QBLoadDialogsCommand extends ServiceCommand {

    private QBPrivateChatHelper privateChatHelper;
    private QBGroupChatHelper multiChatHelper;

    private final String FIELD_DIALOG_TYPE = "type";
    private final String OPERATOR_EQ = "eq";

    private final int FIRST_PAGE_NUMBER = 1;

    // TODO: HACK!
    // This is temporary value,
    // by default MAX count of Dialogs should be !> (DIALOGS_PARTS * ConstsCore.CHATS_DIALOGS_PER_PAGE)
    // it is 200 Dialogs
    private final static int DIALOGS_PARTS = 10; // TODO: need to fix in the second release.

    private static final int KEEP_ALIVE_TIME = 1;
    private static final TimeUnit KEEP_ALIVE_TIME_UNIT = TimeUnit.SECONDS;
    private static int NUMBER_OF_CORES = Runtime.getRuntime().availableProcessors();

    private BlockingQueue<Runnable> threadQueue;
    private ThreadPoolExecutor threadPool;

    public QBLoadDialogsCommand(Context context, QBPrivateChatHelper privateChatHelper, QBGroupChatHelper multiChatHelper, String successAction,
                                String failAction) {
        super(context, successAction, failAction);
        this.multiChatHelper = multiChatHelper;
        this.privateChatHelper = privateChatHelper;
        initThreads();
    }

    public static void start(Context context) {
        Intent intent = new Intent(QBServiceConsts.LOAD_CHATS_DIALOGS_ACTION, null, context, QBService.class);
        context.startService(intent);
    }

    @Override
    public Bundle perform(Bundle extras) throws Exception {
        final ArrayList<ParcelableQBDialog> parcelableQBDialog = new ArrayList<>();

        final Bundle returnedBundle = new Bundle();
        final QBRequestGetBuilder qbRequestGetBuilder = new QBRequestGetBuilder();

        qbRequestGetBuilder.setLimit(ConstsCore.CHATS_DIALOGS_PER_PAGE);
        qbRequestGetBuilder.sortDesc(QBServiceConsts.EXTRA_LAST_MESSAGE_DATE_SENT);

        parcelableQBDialog.addAll(ChatUtils.qbDialogsToParcelableQBDialogs(
                loadAllDialogs(returnedBundle, qbRequestGetBuilder)));

        Bundle bundle = new Bundle();
        bundle.putParcelableArrayList(QBServiceConsts.EXTRA_CHATS_DIALOGS, parcelableQBDialog);

        return bundle;
    }

    private void initThreads() {
        threadQueue = new LinkedBlockingQueue<>();
        threadPool = new ThreadPoolExecutor(NUMBER_OF_CORES, NUMBER_OF_CORES, KEEP_ALIVE_TIME, KEEP_ALIVE_TIME_UNIT, threadQueue);
        threadPool.allowCoreThreadTimeOut(true);
    }

    private boolean loadAllDialogsByType(QBDialogType dialogsType,  Bundle returnedBundle, QBRequestGetBuilder qbRequestGetBuilder, List<QBDialog> allDialogsList, int pageNumber) throws QBResponseException {
        boolean needToLoadMore = false;

        qbRequestGetBuilder.setSkip(allDialogsList.size());
            qbRequestGetBuilder.addRule(FIELD_DIALOG_TYPE, OPERATOR_EQ, dialogsType.getCode());
            List<QBDialog> newDialogsList = dialogsType == QBDialogType.PRIVATE
                    ? getPrivateDialogs(qbRequestGetBuilder, returnedBundle)
                    : getGroupDialogs(qbRequestGetBuilder, returnedBundle);
            allDialogsList.addAll(newDialogsList);
            needToLoadMore = newDialogsList.size() == ConstsCore.CHATS_DIALOGS_PER_PAGE;
            Log.d("QBLoadDialogsCommand", "needToLoadMore = " + needToLoadMore  + "newDialogsList.size() = " + newDialogsList.size());

            if (dialogsType == QBDialogType.GROUP) {
                boolean needClean = (pageNumber == 0);
                tryJoinRoomChatsPage(newDialogsList, needClean);
            }

        return needToLoadMore;
    }

    private List<QBDialog> loadAllDialogs(final Bundle returnedBundle, final QBRequestGetBuilder qbRequestGetBuilder) throws QBResponseException {
        List<QBDialog> allDialogsList = null;
        List<QBDialog> allDialogsListPrivate = new ArrayList<>();
        List<QBDialog> allDialogsListGroup = new ArrayList<>();
        boolean needToLoadMorePrivate = true;
        boolean needToLoadMoreGroup = true;
        int pageNumber = 0;

        final QBRequestGetBuilder qbRequestGetBuilderPrivate = new QBRequestGetBuilder();

        qbRequestGetBuilderPrivate.setLimit(ConstsCore.CHATS_DIALOGS_PER_PAGE);
        qbRequestGetBuilderPrivate.sortDesc(QBServiceConsts.EXTRA_LAST_MESSAGE_DATE_SENT);
        qbRequestGetBuilderPrivate.addRule(FIELD_DIALOG_TYPE, OPERATOR_EQ, QBDialogType.PRIVATE.getCode());

        final QBRequestGetBuilder qbRequestGetBuilderGroup = new QBRequestGetBuilder();

        qbRequestGetBuilderGroup.setLimit(ConstsCore.CHATS_DIALOGS_PER_PAGE);
        qbRequestGetBuilderGroup.sortDesc(QBServiceConsts.EXTRA_LAST_MESSAGE_DATE_SENT);
        qbRequestGetBuilderGroup.addRule(FIELD_DIALOG_TYPE, OPERATOR_EQ, QBDialogType.GROUP.getCode());

        do {
            Future<Boolean> future1 = null;
            Future<Boolean> future2 = null;

            if(needToLoadMorePrivate) {
                Callable<Boolean> callable1 = new Callable<Boolean>() {
                    private List<QBDialog> allDialogsListPrivate;
                    private int pageNumber;

                    public Callable<Boolean> init(List<QBDialog> allDialogsListPrivate, int pageNumber){
                        this.allDialogsListPrivate = allDialogsListPrivate;
                        this.pageNumber = pageNumber;
                        return this;
                    }

                    @Override
                    public Boolean call() throws Exception {
                        return loadAllDialogsByType(QBDialogType.PRIVATE, returnedBundle, qbRequestGetBuilderPrivate, allDialogsListPrivate, pageNumber);
                    }
                }.init(allDialogsListPrivate, pageNumber);

               future1 = threadPool.submit(callable1);
            }

            if(needToLoadMoreGroup) {

                Callable<Boolean> callable2 = new Callable<Boolean>() {
                    private List<QBDialog> allDialogsListGroup;
                    private int pageNumber;

                    public Callable<Boolean> init(List<QBDialog> allDialogsListGroup, int pageNumber){
                        this.allDialogsListGroup = allDialogsListGroup;
                        this.pageNumber = pageNumber;
                        return this;
                    }

                    @Override
                    public Boolean call() throws Exception {
                        return loadAllDialogsByType(QBDialogType.GROUP, returnedBundle, qbRequestGetBuilderGroup, allDialogsListGroup, pageNumber);
                    }
                }.init(allDialogsListGroup, pageNumber);

                future2 = threadPool.submit(callable2);
            }

            try {
                if(needToLoadMorePrivate) {
                    needToLoadMorePrivate = future1.get();
                }
                if(needToLoadMoreGroup) {
                    needToLoadMoreGroup = future2.get();
                }
            } catch (InterruptedException e) {
                Log.d("QBLoadDialogsCommand", e.getMessage());
            } catch (ExecutionException e) {
                Log.d("QBLoadDialogsCommand", e.getMessage());
            }

            pageNumber++;

            if (pageNumber == FIRST_PAGE_NUMBER) {
                sendLoadPageSuccess(new Bundle());
            }

        } while (needToLoadMorePrivate || needToLoadMoreGroup);

        allDialogsList = new ArrayList<>(allDialogsListPrivate.size() + allDialogsListGroup.size());
        allDialogsList.addAll(allDialogsListPrivate);
        allDialogsList.addAll(allDialogsListGroup);

        return allDialogsList;
    }

    private List<QBDialog> getPrivateDialogs(QBRequestGetBuilder qbRequestGetBuilder, Bundle returnedBundle) throws QBResponseException {
        return privateChatHelper.getDialogs(qbRequestGetBuilder, returnedBundle);
    }

    private List<QBDialog> getGroupDialogs(QBRequestGetBuilder qbRequestGetBuilder, Bundle returnedBundle) throws QBResponseException {
        return multiChatHelper.getDialogs(qbRequestGetBuilder, returnedBundle);
    }

    private void tryJoinRoomChatsPage(final List<QBDialog> dialogsList, final boolean needClean) {
        threadPool.execute(new Runnable() {

            @Override
            public void run() {
                multiChatHelper.tryJoinRoomChatsPage(dialogsList, needClean);
            }
        });
    }

    private void sendLoadPageSuccess(Bundle result){
        sendResult(result, successAction);
    }

    private void sendLoadPageFail(Bundle result){
        sendResult(result, failAction);
    }
}