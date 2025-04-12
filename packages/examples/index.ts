import {kstate} from 'kstate'

kstate.fromTopic('topic').reduce<T>((message, key, state, ctx)=>{
    return {
        state,
        reactions: [
            {
                topic: 'topic',
                key: 'key',
                message: {
                    type: 'require'
                    payload: { id }
                }
            }
        ]
    }
})

