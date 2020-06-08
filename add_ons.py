add_ons_not_24 = {
0:[],
1:[],
3:   \
[['Why nothing back?', 'Are you looking after her ?'], 
['Hi sir?', 'I wish to help if possible?', 'Pls let me know ? My pleasure with Tks'], 
['Missing yours reply on this one: SKYPE_CONTENT'],
['Hi', 'Any news for this? SKYPE_CONTENT'], 
['Shall I hear back on my proposal? pls'], 
['??', 'I am in dark.  sir. '], 
['SKYPE_CONTENT', 'I wish to help if possible , so pls let me know Tks']]
,
5:  \
[['Remind you blw still open', 'SKYPE_CONTENT'],
['If we can develop this one?', 'SKYPE_CONTENT'],
['What is going on there?', 'she is gone/ fixed already?'],
['If Vsl fixed ? - missing your replies, guide me'],
['???', 'Are you busying something else?  What’s the intention '],
['Mate , If she firm working something ?'],
['Remind you abv cargo, shall we try it now?'],
['Remind you of blw cargo, any chance?', 'SKYPE_CONTENT'],
['SKYPE_CONTENT', 'Any chance ?', 'Pls don’t left me in the dark !', 'Appreciate !'],
['If you are looking her sir?', 'SKYPE_CONTENT'],
['SKYPE_CONTENT', 'I am trying her with this one, any enlightenments for me on her ?'],
['Been waiting here for sometime , any news on this ? SKYPE_CONTENT ']]
}

add_ons_24 = {
0:[],
1:[],
}

for this_add_ons in [add_ons_not_24, add_ons_24]:
    for i in range(1, 21):
        if i not in this_add_ons.keys():
            this_add_ons[i] = this_add_ons[i-1]
    




