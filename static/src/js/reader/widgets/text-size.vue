<template>
  <widget>
    <span slot="header">Text Size</span>
    <div slot="body">
      <span :class="['text-size-control', 'text-xs', {active: textSize == 'xs'}]" @click="changeTextSize('xs')">Αα</span>
      <span :class="['text-size-control', 'text-sm', {active: textSize == 'sm'}]" @click="changeTextSize('sm')">Αα</span>
      <span :class="['text-size-control', 'text-md', {active: textSize == 'md'}]" @click="changeTextSize('md')">Αα</span>
      <span :class="['text-size-control', 'text-lg', {active: textSize == 'lg'}]" @click="changeTextSize('lg')">Αα</span>
      <span :class="['text-size-control', 'text-xl', {active: textSize == 'xl'}]" @click="changeTextSize('xl')">Αα</span>
    </div>
  </widget>
</template>

<script>
import store from '../../store';
import widget from '../widget';

export default {
  store,
  computed: {
    textSize() {
      return this.$store.state.reader.textSize;
    },
  },
  methods: {
    changeTextSize(size) {
      this.$store.commit('reader/setTextSize', { size });
    },
  },
  components: {
    widget,
  },
};
</script>
